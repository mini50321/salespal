provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  apis = [
    "serviceusage.googleapis.com",
    "secretmanager.googleapis.com",
    "aiplatform.googleapis.com",
    "run.googleapis.com",
    "cloudbuild.googleapis.com",
    "artifactregistry.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
  ]
}

resource "google_project_service" "enabled" {
  for_each = toset(local.apis)

  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

resource "google_service_account" "salespal_app" {
  account_id   = "salespal-app"
  display_name = "SalesPal application"
  project      = var.project_id

  depends_on = [google_project_service.enabled]
}

resource "google_project_iam_member" "salespal_aiplatform" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.salespal_app.email}"
}

resource "google_project_iam_member" "salespal_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.salespal_app.email}"
}

resource "google_secret_manager_secret" "app" {
  for_each = toset(var.secret_ids)

  secret_id = each.key

  replication {
    auto {}
  }

  depends_on = [google_project_service.enabled]
}
