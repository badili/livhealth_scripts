from sentry_sdk import capture_exception
from django.conf import settings
from hashids import Hashids

from rest_framework.authentication import BasicAuthentication
from rest_framework.decorators import action
from rest_framework.exceptions import ValidationError as RestValidationError
from rest_framework.permissions import AllowAny

from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.exceptions import TokenError, InvalidToken
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer

from phone_verify.base import response as phone_verify_response
from phone_verify.api import VerificationViewSet
from phone_verify.serializers import SMSVerificationSerializer, PhoneSerializer
from phone_verify.services import send_security_code_and_generate_session_token

my_hashids = Hashids(min_length=settings.HASH_IDS_LENGTH, salt=settings.SECRET_KEY)

class QuietBasicAuthentication(BasicAuthentication):
    # disclaimer: once the user is logged in, this should NOT be used as a
    # substitute for SessionAuthentication, which uses the django session cookie,
    # rather it can check credentials before a session cookie has been granted.
    def authenticate_header(self, request):
        return 'xBasic realm="%s"' % self.www_authenticate_realm


class SelfRegistrationViewSet(VerificationViewSet):
    @action(detail=False, methods=["POST"], permission_classes=[AllowAny], serializer_class=PhoneSerializer, )
    def register(self, request):
        try:
            inputs = request.data
            if 'phone_number' not in inputs:
                return Response({'message': "Please specify a telephone to use!"}, status=status.HTTP_400_BAD_REQUEST)
            else:
                user = Personnel.objects.filter(tel=inputs['phone_number']).first()
                if user: return Response({'message': "The specified phone is already in use. Please try again."}, status=status.HTTP_400_BAD_REQUEST)

            if 'u_type' not in inputs:
                return Response({'message': "Please specify the user type that we are registering!"}, status=status.HTTP_400_BAD_REQUEST)
            else:
                if inputs['u_type'] not in('farmer', 'vet'):
                    return Response({'message': "Please specify the correct user type that we are registering!"}, status=status.HTTP_400_BAD_REQUEST)

            if 'acceptedTCs' not in inputs:
                return Response({'message': "Please accept the terms and conditions!"}, status=status.HTTP_400_BAD_REQUEST)
            elif inputs['acceptedTCs'] == False:
                return Response({'message': "The user has rejected the Terms and Conditions. Not registering!"}, status=status.HTTP_406_NOT_ACCEPTABLE)
            
            if inputs['u_type'] == 'farmer':
                user_id = farmer_registration(inputs['phone_number'])
            
            elif inputs['u_type'] == 'vet':
                user_id = vet_registration(inputs)

            try:
                transaction.set_autocommit(True)
                serializer = PhoneSerializer(data=request.data)
                response = serializer.is_valid(raise_exception=True)

                session_token = send_security_code_and_generate_session_token(
                    str(serializer.validated_data["phone_number"])
                )
            except Exception as e:
                # delete the entered user
                delete_self_registered_user(user_id, inputs['u_type'])
                return Response({'message': "There was an error while registering the user."}, status=status.HTTP_400_BAD_REQUEST)

            return phone_verify_response.Ok({"session_token": session_token})

        except Exception as e:
            return Response({'message': "There was an error while registering the user."}, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=["POST"], permission_classes=[AllowAny], serializer_class=SMSVerificationSerializer, )
    def verify(self, request):
        try:
            serializer = SMSVerificationSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)
            
            # All is now ok
            # So lets activate the users and a message for the farmer and an email to the vets
            activate_self_registered_user(request.data['phone_number'])
            return phone_verify_response.Ok({'message': "The registration is complete!!"})

        except Exception as e:
            capture_exception(e)
            return Response({'message': "Check the OTP! There was an error while verifying the OTP!"}, status=status.HTTP_400_BAD_REQUEST)


class OTPLoginViewSet(VerificationViewSet, TokenObtainPairView):
    @action(detail=False, methods=["POST"], permission_classes=[AllowAny], serializer_class=PhoneSerializer, )
    def register(self, request):
        try:
            inputs = request.data
            if 'phone_number' not in inputs:
                return Response({'message': "Please specify a telephone to use!"}, status=status.HTTP_400_BAD_REQUEST)
            else:
                # strip the beginning + or 0 if it exists
                phone_no = inputs['phone_number'].lstrip('+0')
                if len(phone_no) != 9 and len(phone_no) != 12:
                    return Response({'message': "Please enter a valid telephone number."}, status=status.HTTP_400_BAD_REQUEST)

                user = Personnel.objects.filter(tel__iendswith=phone_no).first()
                if user is None:
                    return Response({'message': "The entered phone is not registered. Please try again."}, status=status.HTTP_400_BAD_REQUEST)

            # all is ok... convert the phone number to an international number
            phone_no_int = '+254%s' % phone_no if len(phone_no) == 9 else '+%s' % phone_no
            serializer = PhoneSerializer(data={'phone_number': phone_no_int})
            response = serializer.is_valid(raise_exception=True)

            session_token = send_security_code_and_generate_session_token(
                str(serializer.validated_data["phone_number"])
            )

            return phone_verify_response.Ok({"session_token": session_token})

        except RestValidationError as e:
            error_dict = e.detail  # Retrieve error messages as a dictionary
            all_errors = []
            for field, errors in error_dict.items():
                # Loop through each field and its corresponding error messages
                for error in errors:
                    all_errors.append(error)

            return Response({'message': all_errors}, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            sentry_sdk.capture_exception(e)
            return Response({'message': "There was an error while generating an OTP token."}, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=["POST"], permission_classes=[AllowAny], serializer_class=SMSVerificationSerializer, )
    def verify(self, request):
        try:
            serializer = SMSVerificationSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)

            # All is now ok
            # So lets authenticate the user and log the person in
            cur_user = Personnel.objects.get(tel=request.data['phone_number'])
            
            from common_func.registration import user_auth_details
            params = user_auth_details(cur_user.id)
            update_last_login(None, cur_user)

            return phone_verify_response.Ok(params)

        except RestValidationError as e:
            error_dict = e.detail  # Retrieve error messages as a dictionary
            all_errors = []
            for field, errors in error_dict.items():
                # Loop through each field and its corresponding error messages
                for error in errors:
                    all_errors.append(error)

            return Response({'message': all_errors}, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            capture_exception(e)
            return Response({'message': "Check the OTP! There was an error while verifying the OTP!"}, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=["POST"], permission_classes=[AllowAny], serializer_class=SMSVerificationSerializer, )
    def reset(self, request):
        try:
            serializer = SMSVerificationSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)

            cur_user = Personnel.objects.get(tel=request.data['phone_number'])
            # reset the password
            mobile_reg = MobileReg()
            mobile_reg.reset_pass(request.data, cur_user)

            from common_func.registration import user_auth_details
            params = user_auth_details(cur_user.id)
            params['message'] = "The password has been reset successfully"

            return Response(params, status=status.HTTP_200_OK)

        except RestValidationError as e:
            error_dict = e.detail  # Retrieve error messages as a dictionary
            all_errors = []
            for field, errors in error_dict.items():
                # Loop through each field and its corresponding error messages
                for error in errors:
                    all_errors.append(error)

            return Response({'message': all_errors}, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            capture_exception(e)
            return Response({'message': "There was an error resetting the password"}, status=status.HTTP_400_BAD_REQUEST)
