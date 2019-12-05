# from . import authentication, serializers
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated


class SMSQueueView(APIView):
    permission_classes = (IsAuthenticated,)
    # authentication_classes = (authentication.QuietBasicAuthentication,)
    # serializer_class = serializers.UserSerializer

    def post(self, request, *args, **kwargs):
        content = {'message': 'System updated well'}

        return Response(content)

    def get(self, request, *args, **kwargs):
        content = {'message': 'Fetch the items in the SMS queue'}

        return Response(content)
