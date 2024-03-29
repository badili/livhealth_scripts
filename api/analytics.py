import re
import json
import datetime
import numpy as np
import pandas as pd

from django.db.models import Q, Sum, Count, IntegerField, Min, Max, Avg, F, Value
from django.db.models.functions import Substr
from django.db.models.expressions import RawSQL
from django.conf import settings
from django.http import JsonResponse

from rest_framework.views import APIView

from sentry_sdk import capture_exception
from hashids import Hashids

from livhealth_scripts.models import SyndromicIncidences, NDReport, SHReport, Recipients
from livhealth_scripts.odk_forms import OdkForms

class Analytics(APIView):
    def dispatch(self, request, *args, **kwargs):
        if request.method == 'POST':
            if re.search('submissions$', request.path):
                return self.submissions(request, *args, **kwargs)
            elif re.search('subcounty_rankings$', request.path):
                return self.subcounty_rankings(request, *args, **kwargs)
            elif re.search('scvo_rankings$', request.path):
                return self.enumerator_ranking(request, *args, **kwargs)
            elif re.search('cdr_ranking$', request.path):
                return self.cdr_ranking(request, *args, **kwargs)
            elif re.search('cdr_analytics$', request.path):
                return self.cdr_analytics(request, *args, **kwargs)

        
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
                        'x_name': next_level,
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
        all_data['days_7'] = self.analyze_period_data(start_date, cur_object, '%m-%d')

        # 4 weeks
        start_date = today - datetime.timedelta(weeks=4)
        all_data['weeks_4'] = self.analyze_period_data(start_date, cur_object, 'Wk %U')

        # 12 weeks
        start_date = today - datetime.timedelta(weeks=12)
        all_data['weeks_12'] = self.analyze_period_data(start_date, cur_object, 'Wk %U')

        # 6 months
        start_date = today - datetime.timedelta(days=182)
        all_data['months_6'] = self.analyze_period_data(start_date, cur_object, '%b\'%y')

        return all_data

    def analyze_period_data(self, start_date, cur_object, format_):
        today = datetime.date.today()
        all_subms = cur_object.objects.filter(datetime_reported__gte=start_date, datetime_reported__lte=today).annotate(v_count=Value(1, output_field=IntegerField())).order_by('datetime_reported').values('datetime_reported', 'v_count').all()
        subms_pd = pd.DataFrame(all_subms)
        if subms_pd.empty:
            subms_pd = pd.DataFrame(columns=['datetime_reported', 'v_count', 'periods'])

        else:
            subms_pd['periods'] = subms_pd['datetime_reported'].dt.strftime(format_)

        # fill the blanks
        for i in range((today-start_date).days + 1):
            try:
                month_ = (start_date + datetime.timedelta(days=i)).strftime(format_)
            except AttributeError:
                month_ = (start_date + datetime.timedelta(days=i)).strftime(format_)[1]
            
            if not month_ in subms_pd['periods'].values:
                v_date = (start_date + datetime.timedelta(days=i))
                subms_pd.loc[len(subms_pd.index)] = (datetime.datetime.combine(v_date, datetime.datetime.min.time()), 0, (start_date + datetime.timedelta(days=i)).strftime(format_) )

        subms_pd.sort_values('datetime_reported', inplace=True)
        cur_data = subms_pd.groupby('periods', sort=False).sum('v_count').v_count.to_dict()
        cur_data = {str(k):v for k,v in cur_data.items()}

        return cur_data


    def subcounty_rankings(self, request, *args, **kwargs):
        try:
            all_data = {}
            today = datetime.date.today()

            # 1 week
            start_date = today - datetime.timedelta(days=6)
            all_data['days_7'] = self.compute_ranking(start_date)

            # 4 weeks
            start_date = today - datetime.timedelta(weeks=3)
            all_data['weeks_4'] = self.compute_ranking(start_date)

            # 12 weeks
            start_date = today - datetime.timedelta(weeks=11)
            all_data['weeks_12'] = self.compute_ranking(start_date)

            # 6 months ranking
            start_date = today - datetime.timedelta(days=181)
            all_data['months_6'] = self.compute_ranking(start_date)

            return JsonResponse(all_data, status=200, safe=False)

        except Exception as e:
            capture_exception(e)
            if settings.DEBUG: print(str(e))
            return JsonResponse({'message': "Error while fetching the sub county rankings"}, status=500, safe=False)

    def compute_ranking(self, start_date):
        # get the number of records of
        # 1. syndromes
        # 2. nd1
        # 3. zero
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

    def enumerator_ranking(self, request, *args, **kwargs):
        try:
            all_data = {}
            today = datetime.date.today()

            # 1 week
            start_date = today - datetime.timedelta(days=6)
            all_data['days_7'] = self.compute_enum_ranking(start_date)

            # 4 weeks
            start_date = today - datetime.timedelta(weeks=3)
            all_data['weeks_4'] = self.compute_enum_ranking(start_date)

            # 12 weeks
            start_date = today - datetime.timedelta(weeks=11)
            all_data['weeks_12'] = self.compute_enum_ranking(start_date)

            # 6 months ranking
            start_date = today - datetime.timedelta(days=181)
            all_data['months_6'] = self.compute_enum_ranking(start_date)

            return JsonResponse(all_data, status=200, safe=False)

        except Exception as e:
            capture_exception(e)
            if settings.DEBUG: print(str(e))
            return JsonResponse({'message': "Error while fetching the enumerator rankings"}, status=500, safe=False)

    def compute_enum_ranking(self, start_date):
        # get the number of records of
        # 1. syndromes
        # 2. nd1
        # 3. zero
        all_data = {}

        # syndromic
        syndromes_count = SyndromicIncidences.objects.filter(datetime_reported__gte=start_date).values('scvo_reporter').annotate(no_recs=Count('scvo_reporter')).values('no_recs', 'scvo_reporter').all()
        for syn in syndromes_count:
            if syn['scvo_reporter'] not in all_data:
                all_data[syn['scvo_reporter']] = {}

            all_data[syn['scvo_reporter']]['syndromic'] = syn['no_recs']

        # nd1
        nd1_reports_count = NDReport.objects.filter(datetime_reported__gte=start_date, reporter__isnull=False).values('reporter').annotate(no_recs=Count('reporter')).values('no_recs', 'reporter').all()
        for nd in nd1_reports_count:
            if nd['reporter'] not in all_data:
                all_data[nd['reporter']] = {}

            all_data[nd['reporter']]['nd'] = nd['no_recs']

        for reporter, recs in all_data.items():
            if 'nd' not in recs: recs['nd'] = 0
            if 'syndromic' not in recs: recs['syndromic'] = 0

            recs['total'] = recs['nd'] + recs['syndromic']

        # ordering and getting top 5
        to_return = []
        i = 1
        odk_form = OdkForms()
        for name_ in (sorted(all_data, reverse=True, key=lambda name_:all_data[name_]['total'])):
            to_return.append({
                'rank': i,
                'name': odk_form.get_value_from_dictionary(name_),
                'records': all_data[name_]['total']
            })
            i+=1

            if i==11: break

        return to_return

    def cdr_ranking(self, request, *args, **kwargs):
        try:
            all_data = {}
            today = datetime.date.today()

            # 1 week
            start_date = today - datetime.timedelta(days=6)
            all_data['days_7'] = self.compute_cdr_rankings(start_date)

            # 4 weeks
            start_date = today - datetime.timedelta(weeks=3)
            all_data['weeks_4'] = self.compute_cdr_rankings(start_date)

            # 12 weeks
            start_date = today - datetime.timedelta(weeks=11)
            all_data['weeks_12'] = self.compute_cdr_rankings(start_date)

            # 6 months ranking
            start_date = today - datetime.timedelta(days=181)
            all_data['months_6'] = self.compute_cdr_rankings(start_date)

            return JsonResponse(all_data, status=200, safe=False)

        except Exception as e:
            capture_exception(e)
            if settings.DEBUG: print(str(e))
            return JsonResponse({'message': "Error while fetching the CDR rankings"}, status=500, safe=False)

    def compute_cdr_rankings(self, start_date):
        # lets get all the syndromic reports submitted by reporters
        summed_reports=SyndromicIncidences.objects.filter(datetime_reported__gte=start_date).values('reporter').annotate(sum_reports=Count('reporter')).values('reporter', 'sum_reports').order_by('-sum_reports').all()

        i = 1
        rank_data = []
        odk_form = OdkForms()
        for rep in summed_reports:
            reporter_tel = Recipients.objects.get(username=rep['reporter'])
            rank_data.append({
                'name': odk_form.get_value_from_dictionary(rep['reporter']),
                'records': rep['sum_reports'],
                'tel': reporter_tel.cell_no if reporter_tel.cell_no else reporter_tel.alternative_cell_no
            })

            i+=1
            if i>5: break

        return rank_data

    def cdr_analytics(self, request, *args, **kwargs):
        try:
            all_data = {}
            today = datetime.date.today()
            cdr_count = Recipients.objects.filter(designation='cdr').count()
            once_active_cdrs = list(SyndromicIncidences.objects.values('reporter').annotate(sum_reports=Count('reporter')).values('reporter').all().values_list('reporter', flat=True))

            # 1 week
            start_date = today - datetime.timedelta(days=6)
            all_data['days_7'] = self.compute_cdr_analytics(start_date, cdr_count, once_active_cdrs)

            # 4 weeks
            start_date = today - datetime.timedelta(weeks=3)
            all_data['weeks_4'] = self.compute_cdr_analytics(start_date, cdr_count, once_active_cdrs)

            # 12 weeks
            start_date = today - datetime.timedelta(weeks=11)
            all_data['weeks_12'] = self.compute_cdr_analytics(start_date, cdr_count, once_active_cdrs)

            # 6 months ranking
            start_date = today - datetime.timedelta(days=181)
            all_data['months_6'] = self.compute_cdr_analytics(start_date, cdr_count, once_active_cdrs)

            return JsonResponse(all_data, status=200, safe=False)

        except Exception as e:
            capture_exception(e)
            if settings.DEBUG: print(str(e))
            return JsonResponse({'message': "Error while fetching the CDR analytics"}, status=500, safe=False)

    def compute_cdr_analytics(self, start_date, cdr_count, once_active_cdrs):
        cur_period_cdrs = list(SyndromicIncidences.objects.filter(datetime_reported__gte=start_date).values('reporter').annotate(sum_reports=Count('reporter')).values('reporter', 'sum_reports').all().values_list('reporter', flat=True))

        active_cdrs = 0
        dormant_cdrs = 0
        for cdr in once_active_cdrs:
            if cdr in cur_period_cdrs:
                active_cdrs += 1
            else:
                dormant_cdrs += 1

        return {
            'active': (active_cdrs/cdr_count)*100 ,
            'dormant': (dormant_cdrs/cdr_count)*100,
            'non-responsive': ((cdr_count-(active_cdrs+dormant_cdrs))/cdr_count)*100,
            'total_cdrs': cdr_count
        }



