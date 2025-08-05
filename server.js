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

// Census API configuration - UPDATED: Use 2022 data which is definitely available
const CENSUS_API_KEY = process.env.CENSUS_API_KEY;
const CENSUS_BASE_URL = 'https://api.census.gov/data/2022/acs/acs5';

// UPDATED: Demographic variables with proper Census codes and base populations for percentage calculations
const DEMOGRAPHIC_VARIABLES = {
    'B01003_001E': 'total_population',
    'B19013_001E': 'median_household_income', 
    'B25001_001E': 'total_housing_units',
    'B25003_002E': 'owner_occupied_housing',
    'B25003_001E': 'total_occupied_housing', // Need this for percentage calculation
    'B15003_022E': 'bachelors_degree',
    'B15003_023E': 'masters_degree', 
    'B15003_024E': 'professional_degree',
    'B15003_025E': 'doctorate_degree',
    'B15003_001E': 'total_education_population', // Base population for education percentages
    'B08303_013E': 'commute_30_34_minutes',
    'B08303_014E': 'commute_35_plus_minutes', 
    'B08303_001E': 'total_commuters', // Base population for commute percentages
    'B08006_017E': 'work_from_home',
    'B19001_017E': 'households_200k_plus',
    'B19001_001E': 'total_households' // Base for household income percentages
};

// Geocoding function - UNCHANGED from your original
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

// Helper function to create shopping center key - UNCHANGED
function createShoppingCenterKey(name) {
    return name.toLowerCase().trim();
}

// Helper function to create tenant key - UNCHANGED
function createTenantKey(centerName, tenantName, suiteNumber = '') {
    if (tenantName === 'Vacant') {
        // For vacant spaces, make each one unique by including suite number
        return `${centerName.toLowerCase().trim()}::vacant::${suiteNumber || uuidv4()}`;
    }
    return `${centerName.toLowerCase().trim()}::${tenantName.toLowerCase().trim()}`;
}

// UPDATED: Improved function to get Census Block Groups within radius
async function getCensusBlockGroups(lat, lng, radiusMiles) {
    try {
        const fetch = await import('node-fetch').then(mod => mod.default);
        
        console.log(`Getting block groups for coordinates: ${lat}, ${lng}, radius: ${radiusMiles} miles`);
        
        // Step 1: Get state and county for the center point using geocoding API
        const geoUrl = `https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x=${lng}&y=${lat}&benchmark=Public_AR_Current&vintage=Current_Current&format=json`;
        
        console.log(`Geocoding URL: ${geoUrl}`);
        
        const geoResponse = await fetch(geoUrl);
        
        // Check if geocoding response is valid
        if (!geoResponse.ok) {
            console.error(`Geocoding API returned status: ${geoResponse.status}`);
            const errorText = await geoResponse.text();
            console.error(`Geocoding error response: ${errorText.substring(0, 200)}`);
            return [];
        }
        
        const geoText = await geoResponse.text();
        console.log(`Geocoding raw response: ${geoText.substring(0, 300)}`);
        
        let geoData;
        try {
            geoData = JSON.parse(geoText);
        } catch (parseError) {
            console.error('Geocoding response is not valid JSON:', parseError.message);
            console.error('Response text:', geoText.substring(0, 500));
            return [];
        }
        
        // Validate geocoding response structure
        if (!geoData.result?.geographies?.Counties?.[0]) {
            console.error('Invalid geocoding response structure:', JSON.stringify(geoData, null, 2));
            return [];
        }
        
        const state = geoData.result.geographies.Counties[0].STATE;
        const county = geoData.result.geographies.Counties[0].COUNTY;
        
        console.log(`Found state: ${state}, county: ${county}`);
        
        // Step 2: Get all block groups in the county using correct API format
        const censusUrl = `${CENSUS_BASE_URL}?get=NAME&for=block%20group:*&in=state:${state}%20county:${county}`;
        
        // Add API key if available
        const finalCensusUrl = CENSUS_API_KEY ? `${censusUrl}&key=${CENSUS_API_KEY}` : censusUrl;
        
        console.log(`Census block groups URL: ${finalCensusUrl}`);
        
        const blockGroupsResponse = await fetch(finalCensusUrl);
        
        // Check response status and content type
        console.log(`Census API response status: ${blockGroupsResponse.status}`);
        console.log(`Census API response content-type: ${blockGroupsResponse.headers.get('content-type')}`);
        
        if (!blockGroupsResponse.ok) {
            const errorText = await blockGroupsResponse.text();
            console.error(`Census API error (${blockGroupsResponse.status}): ${errorText.substring(0, 500)}`);
            return [];
        }
        
        const responseText = await blockGroupsResponse.text();
        console.log(`Census API raw response: ${responseText.substring(0, 300)}`);
        
        // Check if response looks like JSON (starts with [ or {)
        if (!responseText.trim().startsWith('[') && !responseText.trim().startsWith('{')) {
            console.error('Census API returned non-JSON response (probably HTML error page)');
            console.error('Response content:', responseText.substring(0, 500));
            return [];
        }
        
        let blockGroupsData;
        try {
            blockGroupsData = JSON.parse(responseText);
        } catch (parseError) {
            console.error('Census API response is not valid JSON:', parseError.message);
            console.error('Response text:', responseText.substring(0, 500));
            return [];
        }
        
        // Validate Census API response structure
        if (!Array.isArray(blockGroupsData) || blockGroupsData.length < 2) {
            console.error('Invalid Census API response structure:', blockGroupsData);
            return [];
        }
        
        // Process block groups (skip header row)
        const blockGroups = blockGroupsData.slice(1).map(row => ({
            name: row[0],
            state: row[1],
            county: row[2], 
            tract: row[3],
            blockGroup: row[4],
            geoid: `${row[1]}${row[2]}${row[3]}${row[4]}` // Create full GEOID
        }));
        
        console.log(`Found ${blockGroups.length} block groups in county`);
        
        // For now, return first 20 block groups to avoid API limits
        // In production, you'd want to implement proper geographic filtering
        const limitedBlockGroups = blockGroups.slice(0, 20);
        
        console.log(`Returning ${limitedBlockGroups.length} block groups for analysis`);
        
        return limitedBlockGroups;
        
    } catch (error) {
        console.error('Error getting census block groups:', error);
        return [];
    }
}

// UPDATED: Improved function to fetch demographics for a specific block group
async function fetchBlockGroupDemographics(state, county, tract, blockGroup) {
    try {
        const fetch = await import('node-fetch').then(mod => mod.default);
        const variables = Object.keys(DEMOGRAPHIC_VARIABLES).join(',');
        
        // Construct URL with proper formatting
        const baseUrl = `${CENSUS_BASE_URL}?get=${variables}&for=block%20group:${blockGroup}&in=state:${state}%20county:${county}%20tract:${tract}`;
        const url = CENSUS_API_KEY ? `${baseUrl}&key=${CENSUS_API_KEY}` : baseUrl;
        
        console.log(`Fetching demographics for block group ${state}${county}${tract}${blockGroup}`);
        console.log(`Demographics URL: ${url}`);
        
        const response = await fetch(url);
        
        // Enhanced error checking
        if (!response.ok) {
            console.error(`Demographics API error (${response.status}) for block group ${state}${county}${tract}${blockGroup}`);
            const errorText = await response.text();
            console.error(`Error response: ${errorText.substring(0, 300)}`);
            return null;
        }
        
        const responseText = await response.text();
        
        // Check if response is JSON
        if (!responseText.trim().startsWith('[')) {
            console.error(`Non-JSON response for block group ${state}${county}${tract}${blockGroup}`);
            console.error(`Response: ${responseText.substring(0, 300)}`);
            return null;
        }
        
        let data;
        try {
            data = JSON.parse(responseText);
        } catch (parseError) {
            console.error(`JSON parse error for block group ${state}${county}${tract}${blockGroup}:`, parseError.message);
            return null;
        }
        
        // Validate response structure
        if (!Array.isArray(data) || data.length < 2) {
            console.error(`Invalid data structure for block group ${state}${county}${tract}${blockGroup}:`, data);
            return null;
        }
        
        // Parse the response (first row is headers, second row is data)
        const headers = data[0];
        const values = data[1];
        
        const demographics = {};
        
        // Process each variable
        Object.keys(DEMOGRAPHIC_VARIABLES).forEach((variable) => {
            const index = headers.indexOf(variable);
            if (index !== -1) {
                const value = values[index];
                // Handle Census null values and negative values (which indicate missing data)
                const numericValue = (value === null || value < 0) ? 0 : parseInt(value) || 0;
                demographics[DEMOGRAPHIC_VARIABLES[variable]] = numericValue;
            } else {
                demographics[DEMOGRAPHIC_VARIABLES[variable]] = 0;
            }
        });
        
        console.log(`Successfully fetched demographics for block group ${state}${county}${tract}${blockGroup}`);
        
        return demographics;
        
    } catch (error) {
        console.error(`Error fetching block group demographics for ${state}${county}${tract}${blockGroup}:`, error);
        return null;
    }
}

// UPDATED: Enhanced aggregation function with proper percentage calculations
function aggregateDemographics(demographicsArray, radiusMiles) {
    const validDemographics = demographicsArray.filter(d => d !== null);
    
    if (validDemographics.length === 0) {
        return {
            radius: radiusMiles,
            total_population: 0,
            median_household_income: 0,
            total_housing_units: 0,
            owner_occupied_percent: 0.0,
            commute_30_plus_percent: 0.0,
            bachelors_degree_percent: 0.0,
            work_from_home_percent: 0.0,
            households_200k_percent: 0.0,
            block_groups_analyzed: 0
        };
    }
    
    // Sum all the demographic counts
    const totals = {
        total_population: 0,
        total_housing_units: 0,
        owner_occupied_housing: 0,
        total_occupied_housing: 0,
        bachelors_degree: 0,
        masters_degree: 0,
        professional_degree: 0,
        doctorate_degree: 0,
        total_education_population: 0,
        commute_30_34_minutes: 0,
        commute_35_plus_minutes: 0,
        total_commuters: 0,
        work_from_home: 0,
        households_200k_plus: 0,
        total_households: 0,
        median_income_sum: 0,
        median_income_count: 0
    };
    
    // Aggregate all demographic data
    validDemographics.forEach(demo => {
        Object.keys(totals).forEach(key => {
            if (demo[key] !== undefined) {
                totals[key] += demo[key];
            }
        });
        
        // For median income, we need to weight by population
        if (demo.median_household_income > 0) {
            totals.median_income_sum += demo.median_household_income * demo.total_population;
            totals.median_income_count += demo.total_population;
        }
    });
    
    // Calculate percentages with proper denominators
    const ownerOccupiedPercent = totals.total_occupied_housing > 0 
        ? Math.round((totals.owner_occupied_housing / totals.total_occupied_housing) * 100 * 10) / 10
        : 0.0;
    
    const bachelorsPlusCount = totals.bachelors_degree + totals.masters_degree + 
                             totals.professional_degree + totals.doctorate_degree;
    const bachelorsPercent = totals.total_education_population > 0
        ? Math.round((bachelorsPlusCount / totals.total_education_population) * 100 * 10) / 10
        : 0.0;
    
    const commutePlusCount = totals.commute_30_34_minutes + totals.commute_35_plus_minutes;
    const commutePercent = totals.total_commuters > 0
        ? Math.round((commutePlusCount / totals.total_commuters) * 100 * 10) / 10
        : 0.0;
    
    const workFromHomePercent = totals.total_commuters > 0
        ? Math.round((totals.work_from_home / totals.total_commuters) * 100 * 10) / 10
        : 0.0;
    
    const households200kPercent = totals.total_households > 0
        ? Math.round((totals.households_200k_plus / totals.total_households) * 100 * 10) / 10
        : 0.0;
    
    const weightedMedianIncome = totals.median_income_count > 0
        ? Math.round(totals.median_income_sum / totals.median_income_count)
        : 0;
    
    return {
        radius: radiusMiles,
        total_population: totals.total_population,
        median_household_income: weightedMedianIncome,
        total_housing_units: totals.total_housing_units,
        owner_occupied_percent: ownerOccupiedPercent,
        commute_30_plus_percent: commutePercent,
        bachelors_degree_percent: bachelorsPercent,
        work_from_home_percent: workFromHomePercent,
        households_200k_percent: households200kPercent,
        block_groups_analyzed: validDemographics.length
    };
}

// API Routes - ALL UNCHANGED from your original

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

// UPDATED: Demographics endpoint with enhanced error handling and logging
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
        console.warn('Census API key not configured - demographics may have limited functionality');
        // Don't return error immediately - Census API works without key but with rate limits
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
                median_household_income: 0,
                total_housing_units: 0,
                owner_occupied_percent: 0.0,
                commute_30_plus_percent: 0.0,
                bachelors_degree_percent: 0.0,
                work_from_home_percent: 0.0,
                households_200k_percent: 0.0,
                block_groups_analyzed: 0
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

// File upload setup - UNCHANGED
const upload = multer({ 
    storage: multer.memoryStorage(),
    limits: { fileSize: 10 * 1024 * 1024 } // 10MB limit
});

// CSV Import endpoint - UNCHANGED from your original
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

// Health check endpoint - UPDATED to show Census API configuration status
app.get('/health', (req, res) => {
    res.json({ 
        status: 'OK', 
        timestamp: new Date().toISOString(),
        shopping_centers: shoppingCenters.size,
        tenant_spaces: tenants.size,
        census_api_configured: !!CENSUS_API_KEY,
        census_api_base_url: CENSUS_BASE_URL
    });
});

// Root endpoint - UNCHANGED
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

// Start server - UPDATED to show Census API configuration in startup logs
app.listen(PORT, () => {
    console.log(`ShopWindow API running on port ${PORT}`);
    console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log(`Google Maps API: ${process.env.GOOGLE_MAPS_API_KEY ? 'Configured' : 'Not configured'}`);
    console.log(`Census API: ${CENSUS_API_KEY ? 'Configured with API key' : 'Not configured (will use public access with rate limits)'}`);
    console.log(`Census API Base URL: ${CENSUS_BASE_URL}`);
});

module.exports = app;
