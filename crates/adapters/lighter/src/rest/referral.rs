use crate::error::Result;
use crate::models::referral::{ReferralActionResponse, ReferralCode, UserReferrals};
use crate::rest::client::LighterRestClient;

impl LighterRestClient {
    pub async fn get_user_referrals(
        &self,
        l1_address: &str,
        cursor: Option<i64>,
        auth: Option<&str>,
    ) -> Result<UserReferrals> {
        let cursor = cursor.map(|value| value.to_string());
        let mut query = vec![("l1_address", l1_address)];
        if let Some(ref cursor) = cursor {
            query.push(("cursor", cursor.as_str()));
        }

        match auth {
            Some(auth) => {
                self.get_with_auth("/api/v1/referral/userReferrals", &query, auth)
                    .await
            }
            None => {
                self.get_with_query("/api/v1/referral/userReferrals", &query)
                    .await
            }
        }
    }

    pub async fn get_referral_code(
        &self,
        account_index: i64,
        auth: Option<&str>,
    ) -> Result<ReferralCode> {
        let account_index = account_index.to_string();
        let query = [("account_index", account_index.as_str())];
        match auth {
            Some(auth) => {
                self.get_with_auth("/api/v1/referral/get", &query, auth)
                    .await
            }
            None => self.get_with_query("/api/v1/referral/get", &query).await,
        }
    }

    pub async fn create_referral_code(
        &self,
        account_index: i64,
        auth: Option<&str>,
    ) -> Result<ReferralCode> {
        let account_index = account_index.to_string();
        let form = [("account_index", account_index.as_str())];
        match auth {
            Some(auth) => {
                self.post_form_with_auth("/api/v1/referral/create", &form, auth)
                    .await
            }
            None => self.post_form("/api/v1/referral/create", &form).await,
        }
    }

    pub async fn update_referral_code(
        &self,
        account_index: i64,
        new_referral_code: &str,
        auth: Option<&str>,
    ) -> Result<ReferralActionResponse> {
        let account_index = account_index.to_string();
        let form = [
            ("account_index", account_index.as_str()),
            ("new_referral_code", new_referral_code),
        ];
        match auth {
            Some(auth) => {
                self.post_form_with_auth("/api/v1/referral/update", &form, auth)
                    .await
            }
            None => self.post_form("/api/v1/referral/update", &form).await,
        }
    }

    pub async fn update_referral_kickback(
        &self,
        account_index: i64,
        kickback_percentage: f64,
        auth: Option<&str>,
    ) -> Result<ReferralActionResponse> {
        let account_index = account_index.to_string();
        let kickback_percentage = kickback_percentage.to_string();
        let form = [
            ("account_index", account_index.as_str()),
            ("kickback_percentage", kickback_percentage.as_str()),
        ];
        match auth {
            Some(auth) => {
                self.post_form_with_auth("/api/v1/referral/kickback/update", &form, auth)
                    .await
            }
            None => {
                self.post_form("/api/v1/referral/kickback/update", &form)
                    .await
            }
        }
    }

    pub async fn use_referral_code(
        &self,
        l1_address: &str,
        referral_code: &str,
        x: &str,
        discord: Option<&str>,
        telegram: Option<&str>,
        signature: Option<&str>,
        auth: Option<&str>,
    ) -> Result<ReferralActionResponse> {
        let mut form = vec![
            ("l1_address", l1_address),
            ("referral_code", referral_code),
            ("x", x),
        ];
        if let Some(discord) = discord {
            form.push(("discord", discord));
        }
        if let Some(telegram) = telegram {
            form.push(("telegram", telegram));
        }
        if let Some(signature) = signature {
            form.push(("signature", signature));
        }

        match auth {
            Some(auth) => {
                self.post_form_with_auth("/api/v1/referral/use", &form, auth)
                    .await
            }
            None => self.post_form("/api/v1/referral/use", &form).await,
        }
    }
}
