import GoogleAuth, { GoogleKey } from 'cloudflare-workers-and-google-oauth';

export class GoogleSheetsAuth {
    private accessToken: string | null = null;
    private tokenExpiry: number = 0;

    constructor(private serviceAccountJson: string) {}

    async getSheetsAccessToken(): Promise<string> {
        const now = Math.floor(Date.now() / 1000);
        
        if (this.accessToken && now < this.tokenExpiry) {
            return this.accessToken;
        }

        const scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly'];
        let googleKey: GoogleKey;
        
        try {
            googleKey = JSON.parse(this.serviceAccountJson);
        } catch (e) {
            throw new Error('Invalid GOOGLE_SERVICE_ACCOUNT_JSON');
        }

        const oauth = new GoogleAuth(googleKey, scopes);
        const token = await oauth.getGoogleAuthToken();

        if (!token) {
            throw new Error('Failed to get Google Sheets access token');
        }

        this.accessToken = token;
        this.tokenExpiry = now + 3600 - 60; 

        return this.accessToken;
    }
}
