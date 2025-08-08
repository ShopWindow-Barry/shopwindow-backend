const express = require('express');
const multer = require('multer');
const csv = require('csv-parse');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const { Pool } = require('pg');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// PostgreSQL connection pool
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

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

// Helper function to normalize shopping center types
function normalizeCenterType(centerType) {
    if (!centerType || typeof centerType !== 'string' || centerType.trim() === '') {
        return null;
    }
    
    const typeStr = centerType.trim();
    if (!typeStr) {
        return null;
    }
    
    // Define ALL 9 valid types
    const validTypes = [
        'Super Regional Mall',
        'Regional Mall', 
        'Community Center',
        'Neighborhood Center',
        'Strip/Convenience',
        'Power Center',
        'Lifestyle Center',
        'Factory Outlet',
        'Theme/Festival'
    ];
    
    // Try exact match first (case-insensitive)
    const typeLower = typeStr.toLowerCase();
    for (const validType of validTypes) {
        if (typeLower === validType.toLowerCase()) {
            return validType;
        }
    }
    
    // Try common variations
    if (typeLower.includes('strip') || typeLower.includes('convenience')) {
        return 'Strip/Convenience';
    } else if (typeLower.includes('power')) {
        return 'Power Center';
    } else if (typeLower.includes('lifestyle')) {
        return 'Lifestyle Center';
    } else if (typeLower.includes('community')) {
        return 'Community Center';
    } else if (typeLower.includes('neighborhood') || typeLower.includes('neighbourhood')) {
        return 'Neighborhood Center';
    } else if (typeLower.includes('regional') && !typeLower.includes('super')) {
        return 'Regional Mall';
    } else if (typeLower.includes('super') && typeLower.includes('regional')) {
        return 'Super Regional Mall';
    } else if (typeLower.includes('factory') || typeLower.includes('outlet')) {
        return 'Factory Outlet';
    } else if (typeLower.includes('theme') || typeLower.includes('festival')) {
        return 'Theme/Festival';
    }
    
    // Return the original value if we can't match it
    console.warn(`Unknown shopping center type: ${typeStr}`);
    return typeStr;
}

// Helper function to normalize tenant names
function normalizeTenantName(name) {
    if (!name || name.trim() === '') {
        return { name: 'Vacant', isVacant: true };
    }
    
    const nameStr = String(name).trim();
    const nameLower = nameStr.toLowerCase();
    
    // Check if it's a vacant variation
    if (nameLower.includes('vacant') || nameLower.includes('empty')) {
        // Extract useful qualifiers
        if (nameLower.includes('drive-thru') || nameLower.includes('drive thru')) {
            return { name: 'Vacant (Drive-Thru)', isVacant: true };
        } else if (nameLower.includes('office')) {
            return { name: 'Vacant (Office)', isVacant: true };
        } else if (nameLower.includes('second floor') || nameLower.includes('2nd floor')) {
            return { name: 'Vacant (2nd Floor)', isVacant: true };
        } else if (nameLower.includes('restaurant')) {
            return { name: 'Vacant (Former Restaurant)', isVacant: true };
        } else if (nameLower.includes('subdivide')) {
            return { name: 'Vacant (Will Subdivide)', isVacant: true };
        } else if (nameLower.includes('outparcel')) {
            return { name: 'Vacant (Outparcel)', isVacant: true };
        } else {
            return { name: 'Vacant', isVacant: true };
        }
    }
    
    return { name: nameStr, isVacant: false };
}

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

// Database helper functions
async function findOrCreateCategory(categoryName) {
    if (!categoryName || categoryName.trim() === '') {
        return null;
    }
    
    const client = await pool.connect();
    try {
        // Check if category exists
        const existingCategory = await client.query(
            'SELECT id FROM retail_categories WHERE name = $1',
            [categoryName.trim()]
        );
        
        if (existingCategory.rows.length > 0) {
            return existingCategory.rows[0].id;
        }
        
        // Create new category
        const newCategory = await client.query(
            'INSERT INTO retail_categories (name, level) VALUES ($1, 2) RETURNING id',
            [categoryName.trim()]
        );
        
        return newCategory.rows[0].id;
    } finally {
        client.release();
    }
}

async function findOrCreateTenant(tenantName, categoryId = null, isChain = false) {
    const client = await pool.connect();
    try {
        // Check if tenant exists
        const existingTenant = await client.query(
            'SELECT id FROM tenants WHERE name = $1',
            [tenantName]
        );
        
        if (existingTenant.rows.length > 0) {
            return existingTenant.rows[0].id;
        }
        
        // Create new tenant
        const newTenant = await client.query(
            'INSERT INTO tenants (name, category_id, is_national_chain) VALUES ($1, $2, $3) RETURNING id',
            [tenantName, categoryId, isChain]
        );
        
        return newTenant.rows[0].id;
    } finally {
        client.release();
    }
}

// API Routes

// Get all shopping centers - UPDATED to include center_type
app.get('/api/shopping-centers/', async (req, res) => {
    const client = await pool.connect();
    try {
        const result = await client.query(`
            SELECT 
                id, name, center_type, address_street, address_city, 
                address_state, address_zip, county, municipality,
                owner, property_manager, total_gla,
                ST_Y(location::geometry) as latitude,
                ST_X(location::geometry) as longitude,
                created_at, updated_at
            FROM shopping_centers 
            ORDER BY name
        `);
        
        res.json({
            data: result.rows,
            count: result.rows.length
        });
    } catch (error) {
        console.error('Error fetching shopping centers:', error);
        res.status(500).json({ error: 'Failed to fetch shopping centers' });
    } finally {
        client.release();
    }
});

// Get tenants for a specific shopping center
app.get('/api/shopping-centers/:id/tenants', async (req, res) => {
    const { id } = req.params;
    const client = await pool.connect();
    
    try {
        const result = await client.query(`
            SELECT 
                t.name as tenant_name,
                s.suite_number,
                s.square_footage,
                rc.name as category,
                l.base_rent,
                l.rent_per_sf,
                CASE WHEN t.name LIKE 'Vacant%' THEN true ELSE false END as is_vacant
            FROM leases l
            JOIN spaces s ON l.space_id = s.id
            JOIN tenants t ON l.tenant_id = t.id
            LEFT JOIN retail_categories rc ON t.category_id = rc.id
            WHERE s.shopping_center_id = $1
            AND l.is_active = TRUE
            ORDER BY s.suite_number, t.name
        `, [id]);
        
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching tenants:', error);
        res.status(500).json({ error: 'Failed to fetch tenants' });
    } finally {
        client.release();
    }
});

// Get vacancy statistics for a shopping center
app.get('/api/shopping-centers/:id/vacancy-stats', async (req, res) => {
    const { id } = req.params;
    const client = await pool.connect();
    
    try {
        const result = await client.query(`
            SELECT 
                sc.name as shopping_center_name,
                sc.center_type,
                sc.total_gla,
                COUNT(s.id) as total_spaces,
                COUNT(CASE WHEN t.name LIKE 'Vacant%' THEN 1 END) as vacant_spaces,
                SUM(s.square_footage) as total_sq_ft,
                SUM(CASE WHEN t.name LIKE 'Vacant%' THEN s.square_footage END) as vacant_sq_ft,
                ROUND(
                    COUNT(CASE WHEN t.name LIKE 'Vacant%' THEN 1 END) * 100.0 / NULLIF(COUNT(s.id), 0), 
                    2
                ) as vacancy_rate_by_count,
                ROUND(
                    SUM(CASE WHEN t.name LIKE 'Vacant%' THEN s.square_footage END) * 100.0 / 
                    NULLIF(SUM(s.square_footage), 0), 
                    2
                ) as vacancy_rate_by_sqft
            FROM shopping_centers sc
            JOIN spaces s ON s.shopping_center_id = sc.id
            JOIN leases l ON l.space_id = s.id AND l.is_active = TRUE
            JOIN tenants t ON t.id = l.tenant_id
            WHERE sc.id = $1
            GROUP BY sc.id, sc.name, sc.center_type, sc.total_gla
        `, [id]);
        
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Shopping center not found' });
        }
        
        res.json(result.rows[0]);
    } catch (error) {
        console.error('Error fetching vacancy stats:', error);
        res.status(500).json({ error: 'Failed to fetch vacancy statistics' });
    } finally {
        client.release();
    }
});

// Demographics endpoint (simplified for PostgreSQL)
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
    
    // For now, return a simplified response
    // In production, you'd implement the full Census API integration
    res.json({
        radius: radiusMiles,
        total_population: 25000,
        median_household_income: 75000,
        message: 'Demographics API integration in progress'
    });
});

// File upload setup
const upload = multer({ 
    storage: multer.memoryStorage(),
    limits: { fileSize: 10 * 1024 * 1024 }
});

// CSV Import endpoint - UPDATED for PostgreSQL with center_type support
app.post('/api/import-csv-v3/', upload.single('file'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
    }

    if (!req.file.originalname.endsWith('.csv')) {
        return res.status(400).json({ error: 'File must be a CSV' });
    }

    const client = await pool.connect();
    
    try {
        await client.query('BEGIN');
        
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
            shopping_centers_updated: 0,
            spaces_created: 0,
            tenants_created: 0,
            leases_created: 0,
            geocoded_centers: 0,
            center_types_processed: {},
            errors: []
        };

        // Process each record
        for (let i = 0; i < records.length; i++) {
            const record = records[i];
            const rowNum = i + 2; // Account for header row
            
            try {
                const centerName = record.shopping_center_name?.trim();
                if (!centerName) {
                    stats.errors.push(`Row ${rowNum}: No shopping center name`);
                    continue;
                }

                // Process center type
                const centerTypeRaw = record.center_type?.trim();
                const centerType = normalizeCenterType(centerTypeRaw);
                
                // Track center types processed
                if (centerType) {
                    stats.center_types_processed[centerType] = 
                        (stats.center_types_processed[centerType] || 0) + 1;
                }

                // Check if shopping center exists
                let shoppingCenterId;
                const existingCenter = await client.query(
                    'SELECT id FROM shopping_centers WHERE name = $1',
                    [centerName]
                );
                
                if (existingCenter.rows.length > 0) {
                    // Update existing shopping center
                    shoppingCenterId = existingCenter.rows[0].id;
                    
                    await client.query(`
                        UPDATE shopping_centers 
                        SET center_type = COALESCE($1, center_type),
                            owner = COALESCE($2, owner),
                            property_manager = COALESCE($3, property_manager),
                            total_gla = COALESCE($4, total_gla),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = $5
                    `, [
                        centerType,
                        record.owner?.trim() || null,
                        record.property_manager?.trim() || null,
                        record.total_gla ? parseInt(record.total_gla) : null,
                        shoppingCenterId
                    ]);
                    
                    stats.shopping_centers_updated++;
                } else {
                    // Create new shopping center
                    const street = record.address_street?.trim() || '';
                    const city = record.address_city?.trim() || '';
                    const state = record.address_state?.trim() || 'PA';
                    const zip = record.address_zip?.toString().trim() || '';
                    
                    // Try geocoding (limit to avoid rate limits)
                    let latitude = null, longitude = null;
                    if (street && city && state && stats.geocoded_centers < 50) {
                        const geoData = await geocodeAddress({
                            street, city, state, zip
                        });
                        
                        if (geoData) {
                            latitude = geoData.latitude;
                            longitude = geoData.longitude;
                            stats.geocoded_centers++;
                            
                            // Rate limiting
                            await new Promise(resolve => setTimeout(resolve, 100));
                        }
                    }
                    
                    // Insert shopping center
                    let insertQuery, insertParams;
                    
                    if (latitude && longitude) {
                        insertQuery = `
                            INSERT INTO shopping_centers (
                                name, center_type, address_street, address_city, address_state, 
                                address_zip, county, municipality, owner, 
                                property_manager, total_gla, location
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, ST_GeogFromText($12))
                            RETURNING id
                        `;
                        insertParams = [
                            centerName, centerType, street, city, state, zip,
                            record.county?.trim() || null,
                            record.municipality?.trim() || null,
                            record.owner?.trim() || null,
                            record.property_manager?.trim() || null,
                            record.total_gla ? parseInt(record.total_gla) : null,
                            `POINT(${longitude} ${latitude})`
                        ];
                    } else {
                        insertQuery = `
                            INSERT INTO shopping_centers (
                                name, center_type, address_street, address_city, address_state, 
                                address_zip, county, municipality, owner, 
                                property_manager, total_gla
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                            RETURNING id
                        `;
                        insertParams = [
                            centerName, centerType, street, city, state, zip,
                            record.county?.trim() || null,
                            record.municipality?.trim() || null,
                            record.owner?.trim() || null,
                            record.property_manager?.trim() || null,
                            record.total_gla ? parseInt(record.total_gla) : null
                        ];
                    }
                    
                    const newCenter = await client.query(insertQuery, insertParams);
                    shoppingCenterId = newCenter.rows[0].id;
                    stats.shopping_centers_created++;
                }
                
                // Handle space
                const suiteNumber = record.tenant_suite_number?.trim() || record.suite_number?.trim() || null;
                let squareFootage = null;
                
                if (record.square_footage) {
                    try {
                        squareFootage = parseInt(record.square_footage.toString().replace(/[^\d]/g, ''));
                    } catch (e) {
                        // Ignore parsing errors
                    }
                }
                
                // Check if space exists
                let spaceId;
                if (suiteNumber) {
                    const existingSpace = await client.query(
                        'SELECT id FROM spaces WHERE shopping_center_id = $1 AND suite_number = $2',
                        [shoppingCenterId, suiteNumber]
                    );
                    
                    if (existingSpace.rows.length > 0) {
                        spaceId = existingSpace.rows[0].id;
                        // Update square footage if provided
                        if (squareFootage) {
                            await client.query(
                                'UPDATE spaces SET square_footage = $1 WHERE id = $2',
                                [squareFootage, spaceId]
                            );
                        }
                    } else {
                        const newSpace = await client.query(
                            'INSERT INTO spaces (shopping_center_id, suite_number, square_footage) VALUES ($1, $2, $3) RETURNING id',
                            [shoppingCenterId, suiteNumber, squareFootage]
                        );
                        spaceId = newSpace.rows[0].id;
                        stats.spaces_created++;
                    }
                } else {
                    // Create space without suite number
                    const newSpace = await client.query(
                        'INSERT INTO spaces (shopping_center_id, square_footage) VALUES ($1, $2) RETURNING id',
                        [shoppingCenterId, squareFootage]
                    );
                    spaceId = newSpace.rows[0].id;
                    stats.spaces_created++;
                }
                
                // Handle tenant
                const tenantResult = normalizeTenantName(record.tenant_name);
                const tenantName = tenantResult.name;
                const isVacant = tenantResult.isVacant;
                
                // Get or create category (not for vacant spaces)
                let categoryId = null;
                if (!isVacant && record.retail_category?.trim()) {
                    categoryId = await findOrCreateCategory(record.retail_category.trim());
                }
                
                // Get or create tenant
                const isChain = record.is_chain?.toString().toLowerCase() === 'yes';
                const tenantId = await findOrCreateTenant(tenantName, categoryId, isChain);
                
                if (!isVacant) {
                    stats.tenants_created++;
                }
                
                // Deactivate existing leases for this space
                await client.query(
                    'UPDATE leases SET is_active = FALSE WHERE space_id = $1 AND is_active = TRUE',
                    [spaceId]
                );
                
                // Create new lease
                let baseRent = null;
                if (record.base_rent) {
                    try {
                        baseRent = parseFloat(record.base_rent.toString().replace(/[^\d.-]/g, ''));
                    } catch (e) {
                        // Ignore parsing errors
                    }
                }
                
                let rentPerSf = null;
                if (baseRent && squareFootage && squareFootage > 0) {
                    rentPerSf = baseRent / squareFootage;
                }
                
                await client.query(
                    'INSERT INTO leases (space_id, tenant_id, is_active, base_rent, rent_per_sf) VALUES ($1, $2, $3, $4, $5)',
                    [spaceId, tenantId, true, baseRent, rentPerSf]
                );
                
                stats.leases_created++;
                
                // Log progress every 100 records
                if (i % 100 === 0) {
                    console.log(`Processed ${i + 1}/${records.length} records...`);
                }
                
            } catch (error) {
                const errorMsg = `Row ${rowNum}: ${error.message.substring(0, 100)}`;
                stats.errors.push(errorMsg);
                console.error(errorMsg);
                // Continue processing other records
            }
        }
        
        await client.query('COMMIT');
        
        console.log('Import completed:', stats);
        
        res.json({
            status: 'success',
            message: 'Import completed successfully!',
            details: {
                shopping_centers_created: stats.shopping_centers_created,
                shopping_centers_updated: stats.shopping_centers_updated,
                spaces_created: stats.spaces_created,
                tenants_created: stats.tenants_created,
                leases_created: stats.leases_created,
                geocoded_centers: stats.geocoded_centers,
                center_types_processed: stats.center_types_processed,
                errors: stats.errors.length
            },
            errors: stats.errors.slice(0, 10) // Show first 10 errors
        });
        
    } catch (error) {
        await client.query('ROLLBACK');
        console.error('CSV import error:', error);
        res.status(500).json({ 
            error: 'Failed to process CSV file',
            detail: error.message 
        });
    } finally {
        client.release();
    }
});

// Health check endpoint
app.get('/health', async (req, res) => {
    try {
        const client = await pool.connect();
        const result = await client.query('SELECT COUNT(*) FROM shopping_centers');
        client.release();
        
        res.json({ 
            status: 'OK', 
            timestamp: new Date().toISOString(),
            shopping_centers_count: parseInt(result.rows[0].count),
            database: 'connected',
            census_api_configured: !!CENSUS_API_KEY
        });
    } catch (error) {
        res.status(500).json({ 
            status: 'ERROR',
            timestamp: new Date().toISOString(),
            database: 'disconnected',
            error: error.message
        });
    }
});

// Root endpoint
app.get('/', (req, res) => {
    res.json({ 
        message: 'ShopWindow API - PostgreSQL Backend with Center Type Support',
        version: '1.2.0',
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
    console.log(`Database: ${process.env.DATABASE_URL ? 'Configured' : 'Not configured'}`);
});

module.exports = app;
