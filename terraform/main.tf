#главная настройка, подтягивает библиотеки, в данном случае сорс к датабрикусу 
terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

variable "whl_base_path" {}
variable "whl_file_name" {}
variable "cluster_id" {}
variable "databricks_host" {
  sensitive = true
}
variable "databricks_token" {
  sensitive = true
}

#provider это точка входа
provider "databricks" {
  host = var.databricks_host
  token = var.databricks_token
}

# === JOB ===
resource "databricks_job" "wheel_job" {
  name = "gh-events-wheel-job"
  schedule {
    quartz_cron_expression = "0 0 2 * * ?"
    timezone_id            = "Europe/Kyiv"
  }
  ##################### DAILY INGESTION FROM GIT 
  task {
    task_key = "daily_ingestion_task"
    existing_cluster_id = var.cluster_id
    python_wheel_task {
      package_name = "app"
      entry_point  = "main"
      parameters = ["daily-injection"]
    }
    library {
      whl = "${var.whl_base_path}/${var.whl_file_name}"
    }
  }
  ##################### BRONZE LAYER
  task {
    task_key = "bronze_parsing_task"

    depends_on {
    task_key = "daily_ingestion_task"
  }
    existing_cluster_id = var.cluster_id
    python_wheel_task {
      package_name = "app"
      entry_point  = "main"
      parameters = ["bronze_parsing"]
    }
    library {
      whl = "${var.whl_base_path}/${var.whl_file_name}"
    }
  }
  ##################### SILVER LAYER
  task {
    task_key = "create_event"
    depends_on {
    task_key = "bronze_parsing_task"
  }
    existing_cluster_id = var.cluster_id
    python_wheel_task {
      package_name = "app"
      entry_point  = "main"
      parameters = ["create_event"]
    }
    library {
      whl = "${var.whl_base_path}/${var.whl_file_name}"
    }
  }

   task {
    task_key = "delete_event"
    depends_on {
    task_key = "bronze_parsing_task"
  }
    existing_cluster_id = var.cluster_id
    python_wheel_task {
      package_name = "app"
      entry_point  = "main"
      parameters = ["delete_event"]
    }
    library {
      whl = "${var.whl_base_path}/${var.whl_file_name}"
    }
  }

   task {
    task_key = "issue_comment_event"
    depends_on {
    task_key = "bronze_parsing_task"
  }
    existing_cluster_id = var.cluster_id
    python_wheel_task {
      package_name = "app"
      entry_point  = "main"
      parameters = ["issue_comment_event"]
    }
    library {
      whl = "${var.whl_base_path}/${var.whl_file_name}"
    }
  }

   task {
    task_key = "issues_event"
    depends_on {
    task_key = "bronze_parsing_task"
  }
    existing_cluster_id = var.cluster_id
    python_wheel_task {
      package_name = "app"
      entry_point  = "main"
      parameters = ["issues_event"]
    }
    library {
      whl = "${var.whl_base_path}/${var.whl_file_name}"
    }
  }

   task {
    task_key = "member_event"
    depends_on {
    task_key = "bronze_parsing_task"
  }
    existing_cluster_id = var.cluster_id
    python_wheel_task {
      package_name = "app"
      entry_point  = "main"
      parameters = ["member_event"]
    }
    library {
      whl = "${var.whl_base_path}/${var.whl_file_name}"
    }
  }

   task {
    task_key = "pull_request_event"
    depends_on {
    task_key = "bronze_parsing_task"
  }
    existing_cluster_id = var.cluster_id
    python_wheel_task {
      package_name = "app"
      entry_point  = "main"
      parameters = ["pull_request_event"]
    }
    library {
      whl = "${var.whl_base_path}/${var.whl_file_name}"
    }
  }

   task {
    task_key = "pull_request_rev_event"
    depends_on {
    task_key = "bronze_parsing_task"
  }
    existing_cluster_id = var.cluster_id
    python_wheel_task {
      package_name = "app"
      entry_point  = "main"
      parameters = ["pull_request_rev_event"]
    }
    library {
      whl = "${var.whl_base_path}/${var.whl_file_name}"
    }
  }

   task {
    task_key = "push_event"
    depends_on {
    task_key = "bronze_parsing_task"
  }
    existing_cluster_id = var.cluster_id
    python_wheel_task {
      package_name = "app"
      entry_point  = "main"
      parameters = ["push_event"]
    }
    library {
      whl = "${var.whl_base_path}/${var.whl_file_name}"
    }
  }

   task {
    task_key = "release_event"
    depends_on {
    task_key = "bronze_parsing_task"
  }
    existing_cluster_id = var.cluster_id
    python_wheel_task {
      package_name = "app"
      entry_point  = "main"
      parameters = ["release_event"]
    }
    library {
      whl = "${var.whl_base_path}/${var.whl_file_name}"
    }
  }

   task {
    task_key = "watch_event"
    depends_on {
    task_key = "bronze_parsing_task"
  }
    existing_cluster_id = var.cluster_id
    python_wheel_task {
      package_name = "app"
      entry_point  = "main"
      parameters = ["watch_event"]
    }
    library {
      whl = "${var.whl_base_path}/${var.whl_file_name}"
    }
  }
  ##################### GOLD LAYER
  task {
    task_key = "user_daily_rating"
    depends_on {
    task_key = "issues_event"
  }
    depends_on {
    task_key = "pull_request_event"
  }
    depends_on {
    task_key = "pull_request_rev_event"
  }
    depends_on {
    task_key = "push_event"
  }
    existing_cluster_id = var.cluster_id
    python_wheel_task {
      package_name = "app"
      entry_point  = "main"
      parameters = ["gold_daily_user_rating"]
    }
    library {
      whl = "${var.whl_base_path}/${var.whl_file_name}"
    }
  }

  task {
    task_key = "pr_cycle"
    depends_on {
    task_key = "pull_request_event"
  }
    depends_on {
    task_key = "pull_request_rev_event"
  }
    existing_cluster_id = var.cluster_id
    python_wheel_task {
      package_name = "app"
      entry_point  = "main"
      parameters = ["gold_pr_cycle"]
    }
    library {
      whl = "${var.whl_base_path}/${var.whl_file_name}"
    }
  }
  task {
    task_key = "repo_popularity"
    depends_on {
    task_key = "watch_event"
  }
    existing_cluster_id = var.cluster_id
    python_wheel_task {
      package_name = "app"
      entry_point  = "main"
      parameters = ["gold_repo_popularity"]
    }
    library {
      whl = "${var.whl_base_path}/${var.whl_file_name}"
    }
  }

}

resource "databricks_job" "bootstrap_job" {
  name = "gh-events-bootstrap-job"

  task {
    task_key = "configuration_tables_creation_task"
    existing_cluster_id = var.cluster_id

    python_wheel_task {
      package_name = "app"
      entry_point  = "main"
      parameters   = ["configuration_tables_creation"]
    }

    library {
      whl = "${var.whl_base_path}/${var.whl_file_name}"
    }
  }
  task {
    task_key = "silver_tables_creation_task"
    existing_cluster_id = var.cluster_id

    python_wheel_task {
      package_name = "app"
      entry_point  = "main"
      parameters   = ["silver_tables_creation"]
    }

    library {
      whl = "${var.whl_base_path}/${var.whl_file_name}"
    }
  }
    task {
    task_key = "silver_tables_creation_task_new"
    existing_cluster_id = var.cluster_id
      depends_on {
    task_key = "silver_tables_creation_task"
  }

    python_wheel_task {
      package_name = "app"
      entry_point  = "main"
      parameters   = ["silver_tables_creation"]
    }

    library {
      whl = "${var.whl_base_path}/${var.whl_file_name}"
    }
  }
}