# Github Events Analytics Platform
### Data Engineering Pipeline on Databricks
#### Igor Kovalchuk
#### Python | SQL | Spark | Databricks | Azure | Terraform
#### Data Engineering course
### All successful builds to download 
[![Build](https://github.com/Igorkrakow/gh_events_wheel/actions/workflows/build.yml/badge.svg)](https://github.com/Igorkrakow/gh_events_wheel/actions)


# Launch Instructions:

1. Move file .whl to Databricks storage

2. Set up all configuration in `terraform/config.auto.tfvars`

3. Run:
   ```bash
   cd terraform/
   terraform init
   terraform apply