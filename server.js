const express = require('express');
const multer = require('multer');
const csv = require('csv-parse');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const geolib = require('geolib');
const turf = require('@turf/turf');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static('public')); // Serve static files if needed

// In-memory storage (simple and fast)
let shoppingCenters = new Map(); // key: shopping_center_name, value: center object
let tenants = new Map(); // key: center_name + tenant_name (or unique for vacant), value: tenant object

// Census API configuration
const CENSUS_API_KEY = process.env.CENSUS_API_KEY;
const CENSUS_BASE_URL = 'https://api.census.gov/data/2023/acs/acs5';

// Demographic variables to fetch from Census API
const DEMOGRAPHIC_VARIABLES = {
    'B01003_001E': 'total_population',
    'B19013_001E': 'median_household_income',
    'B25001_001E': 'total_housing_units',
    'B08303_013E': 'commute_30_plus_minutes',
    'B15003_022E': 'bachelors_degree_plus',
    'B25003_002E': 'owner_occupied_housing',
    'B08301_010E': 'work_from_home',
    'B19001_017E': 'households_200k_plus'
};

// Geocoding function
async function geocodeAddress(address) {
    const GOOGLE_API_KEY = process.env.GOOGLE_MAPS_API_KEY;
    if (!GOOGLE_API_KEY) {
        console.warn('No Google Maps API key provided - skipping geocoding');
        return null;
    }

    const fullAddress = `${address.street}, ${address.city}, ${address.state} ${address.zip}`;
    const encodedAddress = encodeURIComponent(fullAddress);
    
    try {
        const fetch = await import('node-fetch').then(mod => mod.default);
        const response = await fetch(
            `https://maps.googleapis.com/maps/api/geocode/json?address=${encodedAddress}&key=${GOOGLE_API_KEY}`
        );
        
        const data = await response.json();
        
        if (data.status === 'OK' && data.results.length > 0) {
            const location = data.results[0].geometry.location;
            return {
                latitude: location.lat,
                longitude: location.lng,
                google_place_id: data.results[0].place_id
            };
        }
    } catch (error) {
        console.error('Geocoding error:', error);
    }
    
    return null;
}

// Helper function to create shopping center key
function createShoppingCenterKey(name) {
    return name.toLowerCase().trim();
}

// Helper function to create tenant key
function createTenantKey(centerName, tenantName, suiteNumber = '') {
    if (tenantName === 'Vacant') {
        // For vacant spaces, make each one unique by including suite number
        return `${centerName.toLowerCase().trim()}::vacant::${suiteNumber || uuidv4()}`;
    }
    return `${centerName.toLowerCase().trim()}::${tenantName.toLowerCase().trim()}`;
}

// Demographic Functions

// Get Census Block Groups within radius
async function getCensusBlockGroups(lat, lng, radiusMiles) {
    try {
        const fetch = await import('node-fetch').then(mod => mod.default);
        
        // Convert miles to meters for turf calculations
        const radiusMeters = radiusMiles * 1609.34;
        
        // Create a point and buffer around it
        const center = turf.point([lng, lat]);
        const buffer = turf.buffer(center, radiusMeters, { units: 'meters' });
        
        // Get state and county for the center point
        const geoResponse = await fetch(
            `https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x=${lng}&y=${lat}&benchmark=2020&vintage=2020&format=json`
        );
        const geoData = await geoResponse.json();
        
        if (!geoData.result || !geoData.result.geographies) {
            throw new Error('Unable to determine census geography');
        }
        
        const state = geoData.result.geographies.States[0].STATE;
        const county = geoData.result.geographies.Counties[0].COUNTY;
        
        // Get all block groups in the county
        const blockGroupsResponse = await fetch(
            `https://api.census.gov/data/2023/acs/acs5?get=NAME&for=block%20group:*&in=state:${state}%20county:${county}&key=${CENSUS_API_KEY}`
        );
        
        if (!blockGroupsResponse.ok) {
            throw new Error('Failed to fetch block groups from Census API');
        }
        
        const blockGroupsData = await blockGroupsResponse.json();
        
        // Filter to only include block groups (skip header row)
        const blockGroups = blockGroupsData.slice(1).map(row => ({
            name: row[0],
            state: row[1],
            county: row[2],
            tract: row[3],
            blockGroup: row[4]
        }));
        
        // For simplicity, return all block groups in the county
        // In production, you'd want to do proper geometric intersection
        return blockGroups;
        
    } catch (error) {
        console.error('Error getting census block groups:', error);
        return [];
    }
}

// Fetch demographics for a specific block group
async function fetchBlockGroupDemographics(state, county, tract, blockGroup) {
    try {
        const fetch = await import('node-fetch').then(mod => mod.default);
        const variables = Object.keys(DEMOGRAPHIC_VARIABLES).join(',');
        
        const url = `${CENSUS_BASE_URL}?get=${variables}&for=block%20group:${blockGroup}&in=state:${state}%20county:${county}%20tract:${tract}&key=${CENSUS_API_KEY}`;
        
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`Census API request failed: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.length < 2) {
            return null; // No data available
        }
        
        // Parse the response (first row is headers, second row is data)
        const headers = data[0];
        const values = data[1];
        
        const demographics = {};
        Object.keys(DEMOGRAPHIC_VARIABLES).forEach((variable, index) => {
            const value = values[index];
            demographics[DEMOGRAPHIC_VARIABLES[variable]] = value === null ? 0 : parseInt(value) || 0;
        });
        
        return demographics;
        
    } catch (error) {
        console.error('Error fetching block group demographics:', error);
        return null;
    }
}

// Aggregate demographics across multiple block groups
function aggregateDemographics(demographicsArray, radiusMiles) {
    const validDemographics = demographicsArray.filter(d => d !== null);
    
    if (validDemographics.length === 0) {
        return {
            radius: radiusMiles,
            total_population: 0,
            median_household_income: 0,
            total_housing_units: 0,
            commute_30_plus_minutes: 0,
            bachelors_degree_plus: 0,
            owner_occupied_housing: 0,
            work_from_home: 0,
            households_200k_plus: 0,
            block_groups_analyzed: 0
        };
    }
    
    // Sum all the counts
    const totals = validDemographics.reduce((acc, demo) => {
        Object.keys(demo).forEach(key => {
            if (key !== 'median_household_income') {
                acc[key] = (acc[key] || 0) + demo[key];
            }
        });
        return acc;
    }, {});
    
    // Calculate weighted median income (simplified approach)
    const incomes = validDemographics
        .map(d => d.median_household_income)
        .filter(income => income > 0);
    
    const medianIncome = incomes.length > 0 
        ? Math.round(incomes.reduce((sum, income) => sum + income, 0) / incomes.length)
        : 0;
    
    return {
        radius: radiusMiles,
        total_population: totals.total_population || 0,
        median_household_income: medianIncome,
        total_housing_units: totals.total_housing_units || 0,
        commute_30_plus_minutes: totals.commute_30_plus_minutes || 0,
        commute_30_plus_percent: totals.total_population > 0 
            ? Math.round((totals.commute_30_plus_minutes / totals.total_population) * 100) 
            : 0,
        bachelors_degree_plus: totals.bachelors_degree_plus || 0,
        bachelors_degree_percent: totals.total_population > 0 
            ? Math.round((totals.bachelors_degree_plus / totals.total_population) * 100) 
            : 0,
        owner_occupied_housing: totals.owner_occupied_housing || 0,
        owner_occupied_percent: totals.total_housing_units > 0 
            ? Math.round((totals.owner_occupied_housing / totals.total_housing_units) * 100) 
            : 0,
        work_from_home: totals.work_from_home || 0,
        work_from_home_percent: totals.total_population > 0 
            ? Math.round((totals.work_from_home / totals.total_population) * 100) 
            : 0,
        households_200k_plus: totals.households_200k_plus || 0,
        households_200k_percent: totals.total_housing_units > 0 
            ? Math.round((totals.households_200k_plus / totals.total_housing_units) * 100) 
            : 0,
        block_groups_analyzed: validDemographics.length
    };
}

// API Routes

// Get all shopping centers with basic info
app.get('/api/shopping-centers/', (req, res) => {
    const centers = Array.from(shoppingCenters.values()).map(center => ({
        id: center.id,
        name: center.name,
        address_street: center.address_street,
        address_city: center.address_city,
        address_state: center.address_state,
        address_zip: center.address_zip,
        county: center.county,
        municipality: center.municipality,
        owner: center.owner,
        property_manager: center.property_manager,
        total_gla: center.total_gla,
        center_type: center.center_type,
        latitude: center.latitude,
        longitude: center.longitude
    }));

    res.json({
        data: centers,
        count: centers.length
    });
});

// Get tenants for a specific shopping center
app.get('/api/shopping-centers/:id/tenants', (req, res) => {
    const centerId = req.params.id;
    const center = Array.from(shoppingCenters.values()).find(c => c.id === centerId);
    
    if (!center) {
        return res.status(404).json({ error: 'Shopping center not found' });
    }

    // Find all tenants for this shopping center
    const centerTenants = Array.from(tenants.values())
        .filter(tenant => tenant.shopping_center_name === center.name)
        .map(tenant => ({
            suite_number: tenant.tenant_suite_number,
            tenant_name: tenant.tenant_name,
            square_footage: tenant.square_footage,
            category: tenant.retail_category,
            base_rent: tenant.base_rent
        }));

    res.json(centerTenants);
});

// Get vacancy statistics for a shopping center
app.get('/api/shopping-centers/:id/vacancy-stats', (req, res) => {
    const centerId = req.params.id;
    const center = Array.from(shoppingCenters.values()).find(c => c.id === centerId);
    
    if (!center) {
        return res.status(404).json({ error: 'Shopping center not found' });
    }

    // Get all spaces for this center
    const centerSpaces = Array.from(tenants.values())
        .filter(tenant => tenant.shopping_center_name === center.name);

    const totalSpaces = centerSpaces.length;
    const vacantSpaces = centerSpaces.filter(space => space.tenant_name === 'Vacant').length;
    const occupiedSpaces = totalSpaces - vacantSpaces;
    const vacancyRate = totalSpaces > 0 ? Math.round((vacantSpaces / totalSpaces) * 100) : 0;

    res.json({
        total_spaces: totalSpaces,
        vacant_spaces: vacantSpaces,
        occupied_spaces: occupiedSpaces,
        vacancy_rate_by_count: vacancyRate
    });
});

// NEW: Get demographics for a radius around a point
app.get('/api/demographics/:lat/:lng/:radius', async (req, res) => {
    const { lat, lng, radius } = req.params;
    const latitude = parseFloat(lat);
    const longitude = parseFloat(lng);
    const radiusMiles = parseFloat(radius);
    
    // Validate inputs
    if (isNaN(latitude) || isNaN(longitude) || isNaN(radiusMiles)) {
        return res.status(400).json({ error: 'Invalid coordinates or radius' });
    }
    
    if (!CENSUS_API_KEY) {
        return res.status(503).json({ error: 'Census API key not configured' });
    }
    
    try {
        console.log(`Fetching demographics for ${latitude}, ${longitude} within ${radiusMiles} miles`);
        
        // Get census block groups within radius
        const blockGroups = await getCensusBlockGroups(latitude, longitude, radiusMiles);
        
        if (blockGroups.length === 0) {
            return res.json({
                radius: radiusMiles,
                error: 'No census data available for this area',
                total_population: 0,
                median_household_income: 0
            });
        }
        
        // Limit to first 10 block groups for performance (in production, you'd want better geographic filtering)
        const limitedBlockGroups = blockGroups.slice(0, 10);
        
        // Fetch demographics for each block group
        const demographicsPromises = limitedBlockGroups.map(bg => 
            fetchBlockGroupDemographics(bg.state, bg.county, bg.tract, bg.blockGroup)
        );
        
        const demographicsArray = await Promise.all(demographicsPromises);
        
        // Aggregate the results
        const aggregatedDemographics = aggregateDemographics(demographicsArray, radiusMiles);
        
        console.log(`Demographics aggregated from ${aggregatedDemographics.block_groups_analyzed} block groups`);
        
        res.json(aggregatedDemographics);
        
    } catch (error) {
        console.error('Demographics API error:', error);
        res.status(500).json({ 
            error: 'Failed to fetch demographic data',
            detail: error.message 
        });
    }
});

// File upload setup
const upload = multer({ 
    storage: multer.memoryStorage(),
    limits: { fileSize: 10 * 1024 * 1024 } // 10MB limit
});

// CSV Import endpoint
app.post('/api/import-csv-v3/', upload.single('file'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
    }

    if (!req.file.originalname.endsWith('.csv')) {
        return res.status(400).json({ error: 'File must be a CSV' });
    }

    try {
        const csvData = req.file.buffer.toString('utf8');
        
        // Parse CSV
        const records = [];
        const parser = csv.parse(csvData, {
            columns: true,
            skip_empty_lines: true,
            trim: true
        });

        for await (const record of parser) {
            records.push(record);
        }

        console.log(`Processing ${records.length} CSV records...`);

        let stats = {
            shopping_centers_created: 0,
            spaces_created: 0,
            tenants_created: 0,
            geocoded_centers: 0,
            errors: 0
        };

        // Process each record
        for (const record of records) {
            try {
                const centerName = record.shopping_center_name?.trim();
                if (!centerName) {
                    stats.errors++;
                    continue;
                }

                const centerKey = createShoppingCenterKey(centerName);

                // Create shopping center if it doesn't exist
                if (!shoppingCenters.has(centerKey)) {
                    const centerId = uuidv4();
                    
                    // Attempt geocoding
                    let geoData = null;
                    if (record.address_street && record.address_city) {
                        geoData = await geocodeAddress({
                            street: record.address_street,
                            city: record.address_city,
                            state: record.address_state || 'PA',
                            zip: record.address_zip
                        });
                        
                        if (geoData) {
                            stats.geocoded_centers++;
                        }
                    }

                    const newCenter = {
                        id: centerId,
                        name: centerName,
                        address_street: record.address_street || '',
                        address_city: record.address_city || '',
                        address_state: record.address_state || 'PA',
                        address_zip: record.address_zip || '',
                        county: record.county || '',
                        municipality: record.municipality || '',
                        owner: record.owner || '',
                        property_manager: record.property_manager || '',
                        total_gla: parseInt(record.total_gla) || null,
                        center_type: record.center_type || 'Not specified',
                        latitude: geoData?.latitude || null,
                        longitude: geoData?.longitude || null,
                        google_place_id: geoData?.google_place_id || record.google_place_id || null
                    };

                    shoppingCenters.set(centerKey, newCenter);
                    stats.shopping_centers_created++;
                    
                    // Add a small delay between geocoding requests to be nice to the API
                    if (geoData) {
                        await new Promise(resolve => setTimeout(resolve, 100));
                    }
                }

                // Create tenant/space record
                const tenantName = record.tenant_name?.trim() || 'Unknown';
                const suiteNumber = record.tenant_suite_number?.trim() || '';
                const tenantKey = createTenantKey(centerName, tenantName, suiteNumber);

                // Check if this tenant already exists (skip duplicates unless vacant)
                if (!tenants.has(tenantKey) || tenantName === 'Vacant') {
                    const newTenant = {
                        id: uuidv4(),
                        shopping_center_name: centerName,
                        tenant_name: tenantName,
                        tenant_suite_number: suiteNumber,
                        square_footage: parseInt(record.square_footage) || null,
                        retail_category: record.retail_category || null,
                        base_rent: parseFloat(record.base_rent) || 0
                    };

                    // For vacant spaces, always use a unique key
                    const finalKey = tenantName === 'Vacant' ? 
                        `${tenantKey}::${Date.now()}::${Math.random()}` : 
                        tenantKey;

                    tenants.set(finalKey, newTenant);
                    stats.spaces_created++;
                    
                    if (tenantName !== 'Vacant') {
                        stats.tenants_created++;
                    }
                }

            } catch (error) {
                console.error('Error processing record:', error);
                stats.errors++;
            }
        }

        console.log('Import completed:', stats);

        res.json({
            message: 'Import completed successfully',
            details: stats
        });

    } catch (error) {
        console.error('CSV import error:', error);
        res.status(500).json({ 
            error: 'Failed to process CSV file',
            detail: error.message 
        });
    }
});

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({ 
        status: 'OK', 
        timestamp: new Date().toISOString(),
        shopping_centers: shoppingCenters.size,
        tenant_spaces: tenants.size,
        census_api_configured: !!CENSUS_API_KEY
    });
});

// Root endpoint
app.get('/', (req, res) => {
    res.json({ 
        message: 'ShopWindow API - Simple & Fast with Demographics',
        version: '1.1.0',
        endpoints: [
            'GET /api/shopping-centers/',
            'GET /api/shopping-centers/:id/tenants',
            'GET /api/shopping-centers/:id/vacancy-stats',
            'GET /api/demographics/:lat/:lng/:radius',
            'POST /api/import-csv-v3/',
            'GET /health'
        ]
    });
});

// Start server
app.listen(PORT, () => {
    console.log(`ShopWindow API running on port ${PORT}`);
    console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log(`Google Maps API: ${process.env.GOOGLE_MAPS_API_KEY ? 'Configured' : 'Not configured'}`);
    console.log(`Census API: ${CENSUS_API_KEY ? 'Configured' : 'Not configured'}`);
});

module.exports = app;
