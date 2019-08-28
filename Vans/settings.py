BOT_NAME = "vans"

SPIDER_MODULES = ["vans.spiders"]
NEWSPIDER_MODULE = "vans.spiders"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

DOWNLOAD_DELAY = 1.5

# Enable or disable extensions
EXTENSIONS = {
   "vans.extension.StatusMailer": 80
}

# Configure item pipelines
ITEM_PIPELINES = {
   "vans.pipelines.VansPipeline": 300,
}

# Information needed to be filled for sending the email
# (STATUSMAILER_RECIPIENTS, MAIL_USER, MAIL_PASS)
MYEXT_ENABLED = True
MYEXT_ITEMCOUNT = 500
STATUSMAILER_RECIPIENTS = [""]

MAIL_HOST = "smtp.gmail.com"
MAIL_PORT = 587
MAIL_USER = ""
MAIL_PASS = ""
