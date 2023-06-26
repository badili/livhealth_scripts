from django.urls import re_path

from rest_framework.routers import DefaultRouter, SimpleRouter
from .authentication import SelfRegistrationViewSet, OTPLoginViewSet
from .analytics import Analytics

otp_router = DefaultRouter(trailing_slash=False)
otp_router.register(r'otp_login', OTPLoginViewSet, basename='otp')
# otp_router.register(r'mobile_reg', RegistrationViewSet, basename='phone')

# AUTHENTICATION
# auth_routes = SimpleRouter()
# auth_routes.register(r'auth/refresh', RefreshViewSet, basename='auth-refresh')


urlpatterns = [
    *otp_router.urls,
    re_path('submissions', Analytics.as_view(), name='submissions'),
    re_path('subcounty_rankings', Analytics.as_view(), name='subcounty_rankings'),
    re_path('scvo_rankings', Analytics.as_view(), name='scvo_rankings'),
    re_path('cdr_ranking', Analytics.as_view(), name='cdr_ranking'),
    re_path('cdr_analytics', Analytics.as_view(), name='cdr_analytics'),
]