// netlify/functions/export_deals.ts

import { Handler } from '@netlify/functions';
import axios from 'axios';
import FormData from 'form-data';
import { stringify } from 'csv-stringify/sync';

const OPTICAL_URL = process.env.OPTICAL_URL;
const OPTICAL_TOKEN = process.env.OPTICAL_TOKEN;

if (!OPTICAL_URL || !OPTICAL_TOKEN) {
    throw new Error('Missing required environment variables: OPTICAL_URL or OPTICAL_TOKEN');
}

/**
 * Fields to fetch for each deal.
 */
const fieldsList = [
    'name',
    'stage',
    'product.name',
    'value',
    'owner.email',
    'organization.name',
    'contact.email',
    'referrer.email',
    'engagement.name',
    'engagement.date',
    'metrics.product.name',
    'metrics.label',
    'metric1_estimated',
    'metric1_actual',
    'metric2_estimated',
    'metric2_actual',
    'metric3_estimated',
    'metric3_actual',
    'metric4_estimated',
    'metric4_actual',
    'notes',
];

/**
 * 1) Fetch deals from Directus/Optical
 */
async function fetchDeals(baseUrl: string, token: string): Promise<any[]> {
    const queryParams = fieldsList.map((f) => `fields[]=${encodeURIComponent(f)}`).join('&');
    const url = `${baseUrl}/items/deals?${queryParams}`;

    const headers = { Authorization: `Bearer ${token}` };
    const response = await axios.get(url, { headers });
    return response.data?.data ?? [];
}

/**
 * Strip HTML tags and decode basic HTML entities
 */
function stripHtmlTags(text: string | undefined): string {
    if (!text) return '';
    // Remove all <...> tags
    const noTags = text.replace(/<[^>]*>/g, '');
    return decodeHTMLEntities(noTags);
}

/**
 * Basic HTML entity decoding
 */
function decodeHTMLEntities(text: string): string {
    return text
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'")
        .replace(/&amp;/g, '&')
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>');
}

/**
 * Safely retrieve nested property by dot-notation
 */
function safeGet(obj: any, path: string, defaultValue: any = ''): any {
    return path.split('.').reduce((acc, key) => {
        if (acc && typeof acc === 'object' && key in acc) {
            return acc[key];
        }
        return defaultValue;
    }, obj);
}

/**
 * Flatten a deal object similarly to the Python approach
 */
function flattenDeal(deal: any): Record<string, any> {
    return {
        name: deal?.name ?? '',
        stage: deal?.stage ?? '',
        product_name: safeGet(deal, 'product.name'),
        value: deal?.value ?? '',
        owner_email: safeGet(deal, 'owner.email'),
        organization_name: safeGet(deal, 'organization.name'),
        contact_email: safeGet(deal, 'contact.email'),
        referrer_email: safeGet(deal, 'referrer.email'),
        engagement_name: safeGet(deal, 'engagement.name'),
        engagement_date: safeGet(deal, 'engagement.date'),
        metrics_product_name: safeGet(deal, 'metrics.product.name'),
        metrics_label: safeGet(deal, 'metrics.label'),
        metric1_estimated: deal?.metric1_estimated ?? '',
        metric1_actual: deal?.metric1_actual ?? '',
        metric2_estimated: deal?.metric2_estimated ?? '',
        metric2_actual: deal?.metric2_actual ?? '',
        metric3_estimated: deal?.metric3_estimated ?? '',
        metric3_actual: deal?.metric3_actual ?? '',
        metric4_estimated: deal?.metric4_estimated ?? '',
        metric4_actual: deal?.metric4_actual ?? '',
        notes: stripHtmlTags(deal?.notes),
    };
}

/**
 * Convert array of deals to CSV in memory
 */
function generateCsvInMemory(deals: any[]): string {
    const columns = [
        'name',
        'stage',
        'product_name',
        'value',
        'owner_email',
        'organization_name',
        'contact_email',
        'referrer_email',
        'engagement_name',
        'engagement_date',
        'metrics_product_name',
        'metrics_label',
        'metric1_estimated',
        'metric1_actual',
        'metric2_estimated',
        'metric2_actual',
        'metric3_estimated',
        'metric3_actual',
        'metric4_estimated',
        'metric4_actual',
        'notes',
    ];
    const flattenedDeals = deals.map(flattenDeal);
    return stringify(flattenedDeals, { header: true, columns });
}

/**
 * Upload the CSV to Directus, ensuring the file goes into folder "Reports"
 */
async function uploadFileInMemory(
    fileBuffer: Buffer,
    fileName: string,
    baseUrl: string,
    token: string,
    mimeType: string = 'text/csv',
): Promise<void> {
    const foldersEndpoint = `${baseUrl}/folders`;
    const filesEndpoint = `${baseUrl}/files`;
    const authHeaders = { Authorization: `Bearer ${token}` };

    // 1) Get or create "Reports" folder
    let folderId: string;
    let res = await axios.get(foldersEndpoint, {
        headers: authHeaders,
        params: { 'filter[name][_eq]': 'Reports' },
    });
    let data = res.data?.data ?? [];

    if (data.length > 0) {
        folderId = data[0].id;
    } else {
        // Create the "Reports" folder if not found
        res = await axios.post(
            foldersEndpoint,
            { name: 'Reports', parent: null },
            { headers: authHeaders },
        );
        folderId = res.data?.data?.id;
    }

    // 2) Check if a file with the same name exists in that folder
    res = await axios.get(filesEndpoint, {
        headers: authHeaders,
        params: {
            'filter[folder][_eq]': folderId,
            'filter[filename_download][_eq]': fileName,
        },
    });
    data = res.data?.data ?? [];

    /**
     * 3) We must specify the folder via the "data" field in the multipart form.
     * Otherwise, Directus might ignore it and place the file in the root folder.
     */
    const formData = new FormData();
    formData.append('file', fileBuffer, { filename: fileName, contentType: mimeType });

    // Send the folder as metadata in the "data" part of the form
    const metadata = { folder: folderId };
    formData.append('data', JSON.stringify(metadata));

    // 4) If the file already exists, PATCH it; otherwise, POST
    if (data.length > 0) {
        const existingFileId = data[0].id;
        await axios.patch(`${filesEndpoint}/${existingFileId}`, formData, {
            headers: {
                ...formData.getHeaders(),
                Authorization: `Bearer ${token}`,
            },
        });
    } else {
        await axios.post(filesEndpoint, formData, {
            headers: {
                ...formData.getHeaders(),
                Authorization: `Bearer ${token}`,
            },
        });
    }
}

/**
 * Netlify Function entry:
 *  1) Fetch deals
 *  2) Generate CSV
 *  3) Upload CSV into "Reports" folder in Directus
 */
export const handler: Handler = async () => {
    try {
        // Build a filename like deals-YYYYMMDD-HHh.csv
        const now = new Date();
        const year = now.getFullYear();
        const month = String(now.getMonth() + 1).padStart(2, '0');
        const day = String(now.getDate()).padStart(2, '0');
        const hour = String(now.getHours()).padStart(2, '0');
        const csvFilename = `deals-${year}${month}${day}-${hour}h.csv`;

        // 1) Fetch deals
        const deals = await fetchDeals(OPTICAL_URL!, OPTICAL_TOKEN!);

        // 2) Generate CSV
        const csvString = generateCsvInMemory(deals);
        const csvBuffer = Buffer.from(csvString, 'utf-8');

        // 3) Upload CSV to "Reports" folder in Directus
        await uploadFileInMemory(csvBuffer, csvFilename, OPTICAL_URL!, OPTICAL_TOKEN!);

        const responseBody = {
            status: 'success',
            file_uploaded: csvFilename,
            record_count: deals.length,
        };

        return {
            statusCode: 200,
            body: JSON.stringify(responseBody),
        };
    } catch (error: any) {
        console.error('Error exporting deals:', error);
        const errorBody = {
            status: 'error',
            error_message: error?.message || String(error),
        };
        return {
            statusCode: 500,
            body: JSON.stringify(errorBody),
        };
    }
};
