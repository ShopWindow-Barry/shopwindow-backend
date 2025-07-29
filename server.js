const express = require('express');
const multer = require('multer');
const csv = require('csv-parse');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static('public')); // Serve static files if needed

// In-memory storage (simple and fast)
let shoppingCenters = new Map(); // key: shopping_center_name, value: center object
let tenants = new Map(); // key: center_name + tenant_name (or unique for vacant), value: tenant object

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
        tenant_spaces: tenants.size
    });
});

// Root endpoint
app.get('/', (req, res) => {
    res.json({ 
        message: 'ShopWindow API - Simple & Fast',
        version: '1.0.0',
        endpoints: [
            'GET /api/shopping-centers/',
            'GET /api/shopping-centers/:id/tenants',
            'GET /api/shopping-centers/:id/vacancy-stats',
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
});

module.exports = app;
