terraform {
  backend "s3" {
    bucket       = "launchpad-capstone-state"
    key          = "dev/dev.tfstate"
    use_lockfile = true
    region       = "eu-north-1"
  }
}
