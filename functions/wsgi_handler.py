import serverless_wsgi
from app import app

def handler(event, context):
    """
    Netlify Functions entry point:
    Routes incoming requests to our Flask WSGI application.
    """
    return serverless_wsgi.handle_request(app, event, context)

