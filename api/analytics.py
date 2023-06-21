import re
import json
import datetime
import numpy as np
import pandas as pd

from django.db.models import Q, Sum, Count, IntegerField, Min, Max, Avg, F
from django.db.models.functions import Substr
from django.db.models.expressions import RawSQL
from django.conf import settings
from django.http import JsonResponse

from rest_framework.views import APIView

from sentry_sdk import capture_exception
from hashids import Hashids

from livhealth_scripts.models import SyndromicIncidences, NDReport, SHReport
from livhealth_scripts.odk_forms import OdkForms

class Analytics(APIView):
    def dispatch(self, request, *args, **kwargs):
        if request.method == 'POST':
            if re.search('submissions$', request.path):
                return self.submissions(request, *args, **kwargs)
            elif re.search('subcounty_rankings$', request.path):
                return self.subcounty_rankings(request, *args, **kwargs)

        
        return JsonResponse({'message': "Unknown path '%s'" % request.path}, status=500, safe=False)
        
        # return super().dispatch(request, *args, **kwargs)

    def submissions(self, request, *args, **kwargs):
        try:
            '''
            t_span = request.POST['t_span']
            if t_span not in ('1wk', '4wk', '12wk', '6mo'):
                return JsonResponse({'message': "Please specify a valid time span"}, status=500, safe=False)
            '''

            syndromes = self.analyze_submissions(SyndromicIncidences)
            nd1s = self.analyze_submissions(NDReport)
            shs = self.analyze_submissions(SHReport)

            all_data = {}

            # now merge all the reports
            for top_level, data_ in syndromes.items():
                all_data[top_level] = []
                i = 1
                for next_level, data1_ in data_.items():
                    all_data[top_level].append({
                        'x': i,
                        'y': [data1_, nd1s[top_level][next_level], shs[top_level][next_level], ]
                    })
                    i += 1

            # accepted time spans
            # 1wk, 4wk, 12wk, 6mo

            return JsonResponse(all_data, status=200, safe=False)

        except Exception as e:
            capture_exception(e)
            if settings.DEBUG: print(str(e))
            return JsonResponse({'message': "Error while fetching the analytics"}, status=500, safe=False)

    def analyze_submissions(self, cur_object):
        # we need all the data....
        all_data = {}
        today = datetime.date.today()

        # 1 week
        start_date = today - datetime.timedelta(days=7)
        all_subms = cur_object.objects.filter(datetime_reported__gte=start_date, datetime_reported__lte=today).values('datetime_reported').all()
        subms_pd = pd.DataFrame(all_subms)
        if subms_pd.empty:
            all_data['days_7'] = {}
        else:
            subms_pd['periods'] = subms_pd['datetime_reported'].astype(str).str[:10]
            all_data['days_7'] = subms_pd.groupby('periods').count().datetime_reported.to_dict()

        # fill the blanks
        for i in range((today-start_date).days + 1):
            date_ = (start_date + datetime.timedelta(days=i)).strftime('%Y-%m-%d')
            if date_ not in all_data['days_7']: all_data['days_7'][date_] = 0

        # 4 weeks
        start_date = today - datetime.timedelta(weeks=4)
        all_subms = cur_object.objects.filter(datetime_reported__gte=start_date, datetime_reported__lte=today).values('datetime_reported').all()
        subms_pd = pd.DataFrame(all_subms)
        if subms_pd.empty:
            all_data['weeks_4'] = {}
        else:
            try:
                subms_pd['periods'] = subms_pd['datetime_reported'].dt.isocalendar().week
            except AttributeError:
                subms_pd['periods'] = subms_pd['datetime_reported'].dt.isocalendar()[1]

            all_data['weeks_4'] = subms_pd.groupby('periods').count().datetime_reported.to_dict()
            all_data['weeks_4'] = {str(k):v for k,v in all_data['weeks_4'].items()}

        # fill the blanks
        for i in range((today-start_date).days + 1):
            try:
                week_ = (start_date + datetime.timedelta(days=i)).isocalendar().week
            except AttributeError:
                week_ = (start_date + datetime.timedelta(days=i)).isocalendar()[1]
                
            if week_ not in all_data['weeks_4']: all_data['weeks_4'][str(week_)] = 0

        # 12 weeks
        start_date = today - datetime.timedelta(weeks=12)
        all_subms = cur_object.objects.filter(datetime_reported__gte=start_date, datetime_reported__lte=today).values('datetime_reported').all()
        subms_pd = pd.DataFrame(all_subms)
        if subms_pd.empty:
            all_data['weeks_12'] = {}
        else:
            try:
                subms_pd['periods'] = subms_pd['datetime_reported'].dt.isocalendar().week
            except AttributeError:
                subms_pd['periods'] = subms_pd['datetime_reported'].dt.isocalendar()[1]
                
            all_data['weeks_12'] = subms_pd.groupby('periods').count().datetime_reported.to_dict()
            all_data['weeks_12'] = {str(k):v for k,v in all_data['weeks_12'].items()}

        # fill the blanks
        for i in range((today-start_date).days + 1):
            try:
                weekn_ = (start_date + datetime.timedelta(days=i)).isocalendar().week
            except AttributeError:
                weekn_ = (start_date + datetime.timedelta(days=i)).isocalendar()[1]
            
            if weekn_ not in all_data['weeks_12']: all_data['weeks_12'][str(weekn_)] = 0

        # 6 months
        start_date = today - datetime.timedelta(days=182)
        all_subms = cur_object.objects.filter(datetime_reported__gte=start_date, datetime_reported__lte=today).values('datetime_reported').all()
        subms_pd = pd.DataFrame(all_subms)
        if subms_pd.empty:
            all_data['months_6'] = {}
        else:
            subms_pd['periods'] = subms_pd['datetime_reported'].astype(str).str[:7]
            all_data['months_6'] = subms_pd.groupby('periods').count().datetime_reported.to_dict()

        # fill the blanks
        for i in range((today-start_date).days):
            month_ = (start_date + datetime.timedelta(days=i)).strftime('%Y-%m')
            if month_ not in all_data['months_6']: all_data['months_6'][month_] = 0

        return all_data

    def subcounty_rankings(self, request, *args, **kwargs):
        try:
            all_data = {}
            today = datetime.date.today()

            # 1 week
            start_date = today - datetime.timedelta(days=7)
            all_data['days_7'] = self.compute_ranking(start_date)

            # 4 weeks
            start_date = today - datetime.timedelta(weeks=4)
            all_data['weeks_4'] = self.compute_ranking(start_date)

            # 12 weeks
            start_date = today - datetime.timedelta(weeks=12)
            all_data['weeks_12'] = self.compute_ranking(start_date)

            # 6 months ranking
            start_date = today - datetime.timedelta(days=182)
            all_data['months_6'] = self.compute_ranking(start_date)

            return JsonResponse(all_data, status=200, safe=False)

        except Exception as e:
            capture_exception(e)
            if settings.DEBUG: print(str(e))
            return JsonResponse({'message': "Error while fetching the analytics"}, status=500, safe=False)

    def compute_ranking(self, start_date):
        # get the number of records of
        # 1. syndromes
        # 2. nd1
        # 3. zero
        # 
        all_data = {}

        # syndromic
        syndromes_count = SyndromicIncidences.objects.filter(datetime_reported__gte=start_date).values('sub_county').annotate(sc_recs=Count('sub_county')).values('sc_recs', 'sub_county').all()
        for syn in syndromes_count:
            if syn['sub_county'] not in all_data:
                all_data[syn['sub_county']] = {}

            all_data[syn['sub_county']]['syndromic'] = syn['sc_recs']

        # ND1
        nd_reports = NDReport.objects.filter(datetime_reported__gte=start_date).values('sub_county').annotate(nd_count=Count('sub_county')).values('nd_count', 'sub_county').all()
        for nd in nd_reports:
            if nd['sub_county'] not in all_data:
                all_data[nd['sub_county']] = {}

            all_data[nd['sub_county']]['nd1'] = nd['nd_count']

        # Zero reports .. we haven't processed zero reports, so we gonna use 0s for now
        '''
        zero_reports = NDReport.objects.filter(datetime_reported__gte=start_date).values('sub_county').annotate(nd_count=Count('sub_county')).values('nd_count', 'sub_county').all()
        for nd in zero_reports:
            if nd['sub_county'] not in all_data:
                all_data[nd['sub_county']] = {}

            all_data[nd['sub_county']]['nd1'] = nd['nd_count']
        '''

        # now iterate through the subcounties and do the math
        odk_form = OdkForms()
        for sc_name in settings.SUB_COUNTIES:
            full_name = odk_form.get_value_from_dictionary(sc_name)
            if sc_name not in all_data:
                all_data[sc_name] = {'syndromic': 0, 'nd1': 0, 'zero': 0, 'total': 0}

            if 'syndromic' not in all_data[sc_name]: all_data[sc_name]['syndromic'] = 0
            if 'nd1' not in all_data[sc_name]: all_data[sc_name]['nd1'] = 0
            all_data[sc_name]['zero'] = 0
            all_data[sc_name]['total'] = all_data[sc_name]['syndromic'] + all_data[sc_name]['nd1'] + all_data[sc_name]['zero']
            
            all_data[sc_name]['subCountyName'] = full_name

        # print(json.dumps(all_data))
        # lets do the ordering
        to_return = []
        i = 1
        for sc_name in (sorted(all_data, reverse=True, key=lambda sc_name:all_data[sc_name]['total'])):
            all_data[sc_name]['rank'] = i
            to_return.append(all_data[sc_name])
            i+=1

        return to_return






