output "service_account_email" {
  value = google_service_account.salespal_app.email
}

output "enabled_apis" {
  value = local.apis
}

output "secret_resource_names" {
  value = [for s in google_secret_manager_secret.app : s.name]
}
