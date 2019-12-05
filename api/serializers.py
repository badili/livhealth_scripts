from rest_framework import serializers
from models import SMSQueue
from django.contrib.auth.models import User


class SMSQueueSerializer(serializers.ModelSerializer):
    """
    Serializing all the data in the SMSQueue
    """
    class Meta:
        model = SMSQueue
        # fields = ('message', 'recepient', 'recepient_no', 'msg_status')


class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ('password', 'first_name', 'last_name', 'email',)
        write_only_fields = ('password',)
        read_only_fields = ('is_staff', 'is_superuser', 'is_active', 'date_joined',)

    def restore_object(self, attrs, instance=None):
        # call set_password on user object. Without this
        # the password will be stored in plain text.
        user = super(UserSerializer, self).restore_object(attrs, instance)
        user.set_password(attrs['password'])
        return user
