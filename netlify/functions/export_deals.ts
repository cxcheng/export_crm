import {Handler} from "@netlify/functions";
import axios from "axios";
import FormData from "form-data";
import {stringify} from "csv-stringify/sync";
import fetch from "node-fetch"; // or a different HTTP client if you prefer


// -----------------------------------------------------------------------------
// CONFIG: Replace with your actual values (or fetch from environment variables)
// -----------------------------------------------------------------------------
const OPTICAL_URL = "https://cms.ppp.staging.optical.gov.sg";
const OPTICAL_TOKEN = "2QhCxtAV_-y0pfpLY5sZ2rRNYcidHp4t";

// -----------------------------------------------------------------------------
// 1. FETCH DEALS
// -----------------------------------------------------------------------------
async function fetchDeals(baseUrl: string, token: string): Promise<any[]> {
    const fieldsList = [
        "name",
        "stage",
        "product.name",
        "value",
        "owner.email",
        "organization.name",
        "contact.email",
        "referrer.email",
        "engagement.name",
        "engagement.date",
        "metrics.product.name",
        "metrics.label",
        "metric1_estimated",
        "metric1_actual",
        "metric2_estimated",
        "metric2_actual",
        "metric3_estimated",
        "metric3_actual",
        "metric4_estimated",
        "metric4_actual",
        "notes",
    ];
    const queryParams = fieldsList.map((f) => `fields[]=${f}`).join("&");
    const url = `${baseUrl}/items/deals?${queryParams}`;

    const headers = {Authorization: `Bearer ${token}`};

    const response = await axios.get(url, {
        headers,
        // Disabling certificate verification is not recommended, but shown here
        // just to match your original Python code's verify=False usage:
        httpsAgent: new (require("https").Agent)({rejectUnauthorized: false})
    });
    return response.data?.data ?? [];
}

// -----------------------------------------------------------------------------
// 2. CSV GENERATION
// -----------------------------------------------------------------------------
function stripHtmlTags(text: string | undefined): string {
    if (!text) return "";
    // Remove all HTML tags
    const noTags = text.replace(/<[^>]*>/g, "");
    // Decode common HTML entities:
    return noTags
        .replace(/&amp;/g, "&")
        .replace(/&lt;/g, "<")
        .replace(/&gt;/g, ">")
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'");
}

function flattenDeal(deal: any): Record<string, string> {
    const safeGet = (obj: any, path: string, defaultVal = "") => {
        const keys = path.split(".");
        let current = obj;
        for (const k of keys) {
            if (typeof current !== "object" || current === null || !(k in current)) {
                return defaultVal;
            }
            current = current[k];
        }
        return current ?? defaultVal;
    };

    return {
        name: deal?.name ?? "",
        stage: deal?.stage ?? "",
        product_name: safeGet(deal, "product.name"),
        value: deal?.value ?? "",
        owner_email: safeGet(deal, "owner.email"),
        organization_name: safeGet(deal, "organization.name"),
        contact_email: safeGet(deal, "contact.email"),
        referrer_email: safeGet(deal, "referrer.email"),
        engagement_name: safeGet(deal, "engagement.name"),
        engagement_date: safeGet(deal, "engagement.date"),
        metrics_product_name: safeGet(deal, "metrics.product.name"),
        metrics_label: safeGet(deal, "metrics.label"),
        metric1_estimated: String(deal?.metric1_estimated ?? ""),
        metric1_actual: String(deal?.metric1_actual ?? ""),
        metric2_estimated: String(deal?.metric2_estimated ?? ""),
        metric2_actual: String(deal?.metric2_actual ?? ""),
        metric3_estimated: String(deal?.metric3_estimated ?? ""),
        metric3_actual: String(deal?.metric3_actual ?? ""),
        metric4_estimated: String(deal?.metric4_estimated ?? ""),
        metric4_actual: String(deal?.metric4_actual ?? ""),
        notes: stripHtmlTags(deal?.notes),
    };
}

function generateCsv(deals: any[]): string {
    const columns = [
        "name",
        "stage",
        "product_name",
        "value",
        "owner_email",
        "organization_name",
        "contact_email",
        "referrer_email",
        "engagement_name",
        "engagement_date",
        "metrics_product_name",
        "metrics_label",
        "metric1_estimated",
        "metric1_actual",
        "metric2_estimated",
        "metric2_actual",
        "metric3_estimated",
        "metric3_actual",
        "metric4_estimated",
        "metric4_actual",
        "notes",
    ];

    const records = deals.map(flattenDeal);

    // Using "csv-stringify/sync" to create CSV
    const csvOutput = stringify(records, {
        header: true,
        columns,
    });
    return csvOutput;
}

// -----------------------------------------------------------------------------
// 3. UPLOAD FILE
// -----------------------------------------------------------------------------
async function uploadCsvToDirectus(
    csvContent: string,
    fileName: string,
    baseUrl: string,
    token: string
) {
    const foldersEndpoint = `${baseUrl}/folders`;
    const filesEndpoint = `${baseUrl}/files`;
    const headers = {Authorization: `Bearer ${token}`};

    // Step A: Get or create Reports folder
    let folderId: string | undefined;
    try {
        const folderRes = await axios.get(foldersEndpoint, {
            headers,
            params: {
                "filter[name][_eq]": "Reports",
            },
            httpsAgent: new (require("https").Agent)({rejectUnauthorized: false}),
        });
        const folderData = folderRes.data?.data ?? [];
        if (folderData.length > 0) {
            folderId = folderData[0].id;
        } else {
            // Create folder
            const createRes = await axios.post(
                foldersEndpoint,
                {name: "Reports", parent: null},
                {
                    headers,
                    httpsAgent: new (require("https").Agent)({
                        rejectUnauthorized: false,
                    }),
                }
            );
            folderId = createRes.data?.data?.id;
        }
    } catch (error: any) {
        throw new Error(
            `Error checking/creating 'Reports' folder: ${error?.response?.data || error}`
        );
    }

    // Step B: Check if a file with the same name already exists
    let existingFileId: string | undefined;
    try {
        const existingFileRes = await axios.get(filesEndpoint, {
            headers,
            params: {
                "filter[folder][_eq]": folderId,
                "filter[filename_download][_eq]": fileName,
            },
            httpsAgent: new (require("https").Agent)({rejectUnauthorized: false}),
        });
        const existingFileData = existingFileRes.data?.data ?? [];
        if (existingFileData.length > 0) {
            existingFileId = existingFileData[0].id;
        }
    } catch (error: any) {
        throw new Error(
            `Error searching for existing file: ${error?.response?.data || error}`
        );
    }

    // Step C: Upload (PATCH if exists, otherwise POST)
    // We'll use form-data for multipart upload
    const formData = new FormData();
    formData.append("folder", folderId!);
    // fileName, file content, and "text/csv" mime type
    formData.append("file", csvContent, {
        filename: fileName,
        contentType: "text/csv",
    });

    try {
        if (existingFileId) {
            // PATCH existing
            await axios.patch(`${filesEndpoint}/${existingFileId}`, formData, {
                headers: {
                    ...headers,
                    ...formData.getHeaders(),
                },
                httpsAgent: new (require("https").Agent)({rejectUnauthorized: false}),
            });
        } else {
            // POST new
            await axios.post(filesEndpoint, formData, {
                headers: {
                    ...headers,
                    ...formData.getHeaders(),
                },
                httpsAgent: new (require("https").Agent)({rejectUnauthorized: false}),
            });
        }
    } catch (error: any) {
        throw new Error(
            `Error uploading/replacing file in Directus: ${error?.response?.data || error}`
        );
    }
}

// -----------------------------------------------------------------------------
// Netlify Function: Handler
// -----------------------------------------------------------------------------
// netlify/functions/export_deals.ts
export const handler: Handler = async (event, context) => {
    try {
        // Read environment variables
        const opticalUrl = process.env.OPTICAL_URL;
        const opticalToken = process.env.OPTICAL_TOKEN;

        if (!opticalUrl || !opticalToken) {
            throw new Error("Missing OPTICAL_URL or OPTICAL_TOKEN environment variables");
        }

        // Example: Make a request to the optical URL using the token
        const response = await fetch(opticalUrl, {
            method: "GET",
            headers: {
                Authorization: `Bearer ${opticalToken}`,
            },
        });

        if (!response.ok) {
            // If the request was not successful, throw an error to see details in logs
            const errorText = await response.text();
            throw new Error(`Request failed: ${response.status} - ${errorText}`);
        }

        // Parse the response from the external service
        const data = await response.json();

        // Return a successful response
        return {
            statusCode: 200,
            body: JSON.stringify({
                message: "Export deals successful",
                data,
            }),
        };
    } catch (error) {
        console.error("Error in export_deals function:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: "An error occurred in export_deals",
                error: (error as Error).message || error,
            }),
        };
    }
};
