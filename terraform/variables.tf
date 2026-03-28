variable "project_id" {
  type        = string
  description = "Existing GCP project ID (create project and attach billing in Cloud Console first)."
}

variable "region" {
  type    = string
  default = "asia-south1"
}

variable "secret_ids" {
  type = list(string)
  default = [
    "zo_crm_client_secret",
    "zo_crm_refresh_token",
    "whatsapp_access_token",
    "whatsapp_app_secret",
    "whatsapp_verify_token",
    "tata_voice_api_key",
    "smtp_password",
    "sms_api_key",
  ]
}
