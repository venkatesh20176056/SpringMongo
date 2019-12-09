import pytz


TENANT_TIMEZONES = {
    'paytm-india': pytz.timezone('Asia/Kolkata'),
    'paytm-canada': pytz.timezone('America/Toronto'),
}

TENANTS = TENANT_TIMEZONES.keys()
