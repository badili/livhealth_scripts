import os
import json
import logging
import traceback
import sentry_sdk
import datetime as dt
import numpy as np
import pandas as pd
import math
import boto3
import matplotlib.pyplot as plt
import re

from dateutil.relativedelta import relativedelta
from django.db import transaction
from django.db.models import Q, Sum, Count, IntegerField, Min, Max, Avg, F
from django.db.models.expressions import RawSQL
from django.conf import settings
from django.utils import timezone
from django.views.generic import TemplateView
from calendar import monthrange

from wkhtmltopdf.views import PDFTemplateResponse, PDFTemplateView
from hashids import Hashids
from botocore.errorfactory import ClientError

from .models import SyndromicIncidences, SyndromicDetails, NDReport, NDDetail, SHReport, SHSpecies, SHParts, AGReport, AGDetail, DictionaryItems

from .terminal_output import Terminal

terminal = Terminal()
sentry_sdk.init(settings.SENTRY_DSN, environment=settings.ENV_ROLE)
my_hashids = Hashids(min_length=5, salt=settings.SECRET_KEY)


def report_wrapper(request, hashid):
    try:
        grapher = GraphsGenerator()
        report_details = grapher.report_details(hashid)
        
        filename_ = '%s for %s.pdf' % (report_details['title'], report_details['period'])
        template_name_ = report_details['report_template']
        
        header_template_ = 'reports/header.html'
        footer_template_ = 'reports/footer.html'
        cmd_options_ = { 'margin-top': 3, 'disable-smart-shrinking': True }

        report_vars = {
            'report_title': '%s %s' % (report_details['title'], report_details['period']),
            'report_period': 'January',
            'details': report_details
        }

    except Exception as e:
        if settings.DEBUG: terminal.tprint(str(e), 'fail')
        sentry_sdk.capture_exception(e)
        raise Exception('There was an error while fetching the report')



    class ReportView(PDFTemplateView):
        filename = filename_
        template_name = template_name_

        def get_context_data(self, **kwargs):
            context = super(ReportView, self).get_context_data(**kwargs)
            context = {**context, **report_vars}

            return context
        
        
    return ReportView.as_view()(request)
    # request=request, filename=filename_, cmd_options={'disable-javascript': False}, template_name=template_name_, context=context
    # context = RequestContext(request)    
    # return PDFTemplateView.as_view()(request=request, template_name=template_name_)


class GraphsGenerator():
    # the graphs that we are going to generate
    graph_names = ['reports', 'scvo_reporters', 'reports_trend', 'cdr_reporters', 'disease_distibution']

    """
        graph periods coding

        0 - get the full year
        1-12  for jan to dec
        13-16 for q1 to q4
        17 for half 1
        18 for half 2
    """
    t_period = {
        'fm': {'name': 'Month'}, 'fq': {'name': 'Quarter'}, 'fh': {'name': 'Half Year'}, 'fy': {'name': 'Year'}
    }

    def __init__(self):
        print('Silence is golden')

    def report_details(self, hashid):
        """
            the coded hashid contains the year and the period of the report
            the period is 0-18 according to the period coding
        """

        decoded = my_hashids.decode(hashid)
        # print(my_hashids.encode(2020, 0))
        
        g_year = decoded[0]
        g_period = decoded[1] if len(decoded) == 2 else None

        report_details = {
            'graphs': {},
            'title': '%s LivHealth Disease Surveillance Report' % settings.COUNTY_NAME,
            'period_year': g_year,
            'total_reports': 'xx',
            'leading_subcounty': 'sub county',
            'highest_no_reports': 'yy'
        }

        period_code = None
        if g_period == 0:
            report_period = 'Year'
            report_details['period_type'] = 'year'
            report_details['report_template'] = 'reports/monthly_report.html'
            report_details['first_date'] = dt.date(g_year, 1, 1)
            period_code = 'fy'

        elif g_period < 13:
            report_period = dt.date(g_year, g_period, 1).strftime("%B")
            report_details['period_type'] = 'Month'
            report_details['report_template'] = 'reports/monthly_report.html'
            report_details['first_date'] = dt.date(g_year, g_period, 1)
            period_code = 'fm'

        elif g_period < 17:
            report_period = 'Quarter %d,' % (g_period - 12)
            report_details['period_type'] = 'Quarter'
            report_details['report_template'] = 'reports/monthly_report.html'
            report_details['first_date'] = dt.date(g_year, ((g_period -12) * 3) - 2, 1)
            period_code = 'fq'

        else:
            report_period = '%d Half of ' % (g_period - 16)
            report_details['period_type'] = 'Half Year'
            report_details['report_template'] = 'reports/monthly_report.html'
            report_details['first_date'] = dt.date(g_year, ((g_period - 16) * 6) - 5, 1)
            period_code = 'fh'

        report_details['extra_info'] = {}
        # get the period date details
        self.determine_graphs_period(report_details['first_date'])
        period_ = self.t_period[period_code]
        self.report_extra_details(period_)
        
        for r_type in self.graph_names:
            if r_type == 'disease_distibution':
                # we have multiple graphs here
                all_species = list(SyndromicDetails.objects.select_related('incidence').filter(incidence__datetime_reported__gte=period_['start']).filter(incidence__datetime_reported__lte=period_['end']).values('species').distinct('species'))
                for specie in all_species:
                    report_details['graphs']['%s_%s' % (r_type, specie['species'])] = "%sreports/%s_disease_distibution_%s_%d_%d.jpg" % (settings.STATIC_URL, settings.PROJECT_NAME, specie['species'], g_year, g_period )

                report_details['extra_info']['all_species'] = all_species
            else:
                report_details['graphs'][r_type] = "%s/reports/%s_%s_%d_%d.jpg" % (settings.STATIC_URL, settings.PROJECT_NAME, r_type, g_year, g_period )

        
        report_details['period'] = '%s %s' % (report_period, g_year)
        report_details['period_name'] = report_period


        return report_details

    def report_extra_details(self, period_):
        # get the additional details for this report

        # get the species whose graphs have been generated for the disease distribution
        to_return = {}

        return to_return

    def determine_graphs_period(self, report_date = None):
        try:
            # when called, determine the period to use in generating the graphs
            
            if report_date is None: report_date = dt.datetime.now()

            # get the interested complete month
            self.t_period['fm']['year'] = report_date.year if report_date.month > 0 else report_date.year - 1
            self.t_period['fm']['no'] = 12 if report_date.month == 1 else report_date.month-1
            self.t_period['fm']['start'] = dt.date(self.t_period['fm']['year'], self.t_period['fm']['no'], 1)
            self.t_period['fm']['end'] = self.t_period['fm']['start']+relativedelta(months=1, days=-1)
            self.t_period['fm']['gid'] = self.t_period['fm']['no']
            self.t_period['fm']['period_name'] = self.t_period['fm']['start'].strftime('%B')

            # get the interested complete quarter
            curr_quarter = 1+(report_date.month-1)//3
            self.t_period['fq']['year'] = report_date.year if curr_quarter > 1 else report_date.year - 1
            self.t_period['fq']['no'] = 4 if curr_quarter == 1 else curr_quarter - 1
            self.t_period['fq']['start'] = dt.date(self.t_period['fq']['year'], 1+3*(self.t_period['fq']['no']-1), 1)
            self.t_period['fq']['end'] = self.t_period['fq']['start'] + relativedelta(months=3, days=-1)
            self.t_period['fq']['gid'] = self.t_period['fq']['no'] + 12
            self.t_period['fq']['period_name'] = 'Quarter %d of %s' % (self.t_period['fq']['no'], self.t_period['fq']['year'])

            # get the interested complete half year
            curr_half = 1+(report_date.month-1)//6
            self.t_period['fh']['year'] = report_date.year if curr_half > 1 else report_date.year - 1
            self.t_period['fh']['no'] = 2 if curr_half == 1 else curr_half - 1
            self.t_period['fh']['start'] = dt.date(self.t_period['fh']['year'], 1+6*(self.t_period['fh']['no']-1), 1)
            self.t_period['fh']['end'] = self.t_period['fh']['start']+relativedelta(months=6, days=-1)
            self.t_period['fh']['gid'] = self.t_period['fh']['no'] + 16
            self.t_period['fh']['period_name'] = '%d Half of %s' % (self.t_period['fh']['no'], self.t_period['fh']['year'])

            # get the previous year
            self.t_period['fy']['year'] = report_date.year - 1
            self.t_period['fy']['no'] = ''
            self.t_period['fy']['start'] = dt.date(self.t_period['fy']['year'], 1, 1)
            self.t_period['fy']['end'] = dt.date(self.t_period['fy']['year'], 12, 31)
            self.t_period['fy']['gid'] = 0
            self.t_period['fy']['period_name'] = self.t_period['fy']['year']

            print(self.t_period)

        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry_sdk.capture_exception(e)
            raise Exception('There was an error while initializing the graphs generator')

    def generate_graphs(self):
        try:
            for key_, period_ in self.t_period.items():
                for r_type in self.graph_names:
                    file_name_path = "reports/%s_%s_%d_%d.jpg" % (settings.PROJECT_NAME, r_type, period_['year'], period_['gid'] )
                    
                    try:
                        if settings.USE_S3 == 'True':
                            s3 = boto3.client('s3')
                            s3.head_object(Bucket=settings.AWS_STORAGE_BUCKET_NAME, Key=file_name_path)
                        else:
                            if not os.path.exists("%s/%s" % (settings.STATIC_ROOT, file_name_path)): raise Exception('The file %s is missing' % file_name_path)

                    except:
                        # Not found
                        print("\nGenerate the '%s' report for %s%s" % (r_type, period_['year'], '' if key_ == 'fy' else '_%s%d' % (key_, period_['no'])) )
                        self.fetch_graph_reports(period_, r_type, file_name_path)

        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry_sdk.capture_exception(e)
            raise Exception('There was an error while generating the graphs')

    def fetch_graph_reports(self, period_, r_type, file_name_path):
        if r_type == 'reports': self.fetch_graph_received_reports(period_, file_name_path)
        elif r_type == 'scvo_reporters': self.fetch_graph_scvo_reporters(period_, file_name_path)
        elif r_type == 'reports_trend': self.fetch_graph_reports_trend(period_, file_name_path)
        elif r_type == 'cdr_reporters': self.fetch_graph_cdr_reporters(period_, file_name_path)
        elif r_type == 'disease_distibution':
            # get the species in this period
            all_species = list(SyndromicDetails.objects.select_related('incidence').filter(incidence__datetime_reported__gte=period_['start']).filter(incidence__datetime_reported__lte=period_['end']).values('species').distinct('species'))
            
            for specie in all_species:
                file_name_path = "reports/%s_disease_distibution_%s_%d_%d.jpg" % (settings.PROJECT_NAME, specie['species'], period_['year'], period_['gid'] )
                self.fetch_graph_disease_distibution(period_, specie['species'], file_name_path)

    def fetch_graph_received_reports(self, period_, file_name_path):
        """
            Fetch, analyse and graph data for the received reports
        """

        rp_df = pd.DataFrame()
        rp_df['sub_counties'] = settings.SUB_COUNTIES

        # syndromic records per sub county
        sr_df = pd.DataFrame(list( SyndromicIncidences.objects.filter(datetime_reported__gte=period_['start']).filter(datetime_reported__lte=period_['end']).values('sub_county').annotate(no_reports=Count('sub_county')) ))
        if not sr_df.empty:
            recs = sr_df.set_index('sub_county').T.to_dict('records')[0]
            rp_df['syndromics'] = [recs[sc] if sc in recs.keys() else 0 for sc in settings.SUB_COUNTIES]
        
        else: rp_df['syndromics'] = [0]* len(settings.SUB_COUNTIES)
        
        # notifiable records records per sub county
        nddetail = NDDetail.objects.select_related('nd_report').filter(nd_report__datetime_reported__gte=period_['start']).filter(nd_report__datetime_reported__lte=period_['end']).values('nd_report__sub_county').annotate(no_reports=Count('nd_report__sub_county'))
        ndr_df = pd.DataFrame(list( nddetail ))
        if not ndr_df.empty:
            recs = ndr_df.set_index('nd_report__sub_county').T.to_dict('records')[0]
            rp_df['nd1'] = [recs[sc] if sc in recs.keys() else 0 for sc in settings.SUB_COUNTIES]
        
        else: rp_df['nd1'] = [0]* len(settings.SUB_COUNTIES)

        # zero reports per sub county
        zeros = NDReport.objects.filter(datetime_reported__gte=period_['start']).filter(datetime_reported__lte=period_['end']).filter(nddetail=None).values('sub_county').annotate(no_reports=Count('sub_county'))
        zeros_df = pd.DataFrame(list( zeros ))
        if not zeros_df.empty:
            recs = zeros_df.set_index('sub_county').T.to_dict('records')[0]
            rp_df['zero'] = [recs[sc] if sc in recs.keys() else 0 for sc in settings.SUB_COUNTIES]
            
        else: rp_df['zero'] = [0]* len(settings.SUB_COUNTIES)

        rp_df['total'] = rp_df.syndromics + rp_df.nd1 + rp_df.zero
        rp_df.sort_values('total', ascending=False, inplace=True)
        
        data_cols = ['syndromics', 'nd1', 'zero']
        data_labels = ['Syndromic Records', 'Notifiable Diseases', 'Zero Reports']

        # calculate the %-ages for the data columns
        for i in data_cols:
            rp_df['{}_perc'.format(i)] = rp_df[i] / rp_df['total']
        

        fig, ax = plt.subplots(1, figsize=(12, 8))

        bottom = len(settings.SUB_COUNTIES) * [0]
        for i, col_ in enumerate(data_cols):
            col = '%s_perc' % col_
            ax.bar(settings.SUB_COUNTIES, rp_df[col_], 0.35, label=data_labels[i], bottom=bottom)
            bottom = bottom + rp_df[col_]

        
        ax.set_ylabel('No of Records')
        ax.set_title('Sub County Reporting for %s %s %d' % (period_['name'], period_['no'], period_['year']))
        

        ax.legend(frameon=False, loc='best', fontsize='small')      # show the legend
        ax.yaxis.grid(color='gray', linestyle='dashed', linewidth=0.5, alpha=0.5)             # show the grid lines
        plt.xlim(auto=True)

        if settings.USE_S3 == 'True':
            # push it to s3
            client = boto3.client('s3', region_name=settings.AWS_S3_REGION_NAME)
            img_name = '%s.png' % access_code
            img.save(img_name)
            client.upload_file(img_name, settings.AWS_STORAGE_BUCKET_NAME, 'static/qr_codes/%s' % img_name, ExtraArgs={'ACL':'public-read'})
        else:
            plt.savefig(fname="%s/%s" % (settings.STATIC_ROOT, file_name_path))

        plt.close(fig)

    def fetch_graph_scvo_reporters(self, period_, file_name_path):
        # fetch and graph data on number of reports per sub county vets
        # This report is critical for morale building, but the data is not yet extracted and saved in the database
        print()

    def fetch_graph_reports_trend(self, period_, file_name_path):
        # fetch and graph data on the trend of received reports

        rp_df = pd.DataFrame()
        # determine the number items that we are expecting
        grp_periods = []

        # number of syndromic records per sub county
        if period_['gid'] == 0:
            si = SyndromicIncidences.objects.filter(datetime_reported__gte=period_['start']).filter(datetime_reported__lte=period_['end']).annotate(grp_period=RawSQL('EXTRACT(MONTH FROM datetime_reported)', []) ).values('grp_period').annotate(no_reports=Count('grp_period')).order_by('grp_period')
            grp_periods.extend(range(1, 13))
            grp_periods_name = 'Months'

        elif period_['gid'] < 13:
            si = SyndromicIncidences.objects.filter(datetime_reported__gte=period_['start']).filter(datetime_reported__lte=period_['end']).annotate(grp_period=RawSQL('DATE(datetime_reported)', []) ).values('grp_period').annotate(no_reports=Count('grp_period')).order_by('grp_period')
            dates_ = []
            dates_.extend(range(1, monthrange(period_['year'], period_['gid'])[1] + 1))
            grp_periods = [dt.datetime(2020, 2, x).strftime('%d') for x in dates_]
            grp_periods_name = 'Dates'
        else:
            si = SyndromicIncidences.objects.filter(datetime_reported__gte=period_['start']).filter(datetime_reported__lte=period_['end']).annotate(grp_period=RawSQL('EXTRACT(WEEK FROM datetime_reported)', []) ).values('grp_period').annotate(no_reports=Count('grp_period')).order_by('grp_period')
            grp_periods_name = 'Week Nos'

            # get the weeks in this range
            no_wks = math.ceil((period_['end'] - period_['start']) / dt.timedelta(weeks=1))
            start_wk = period_['start'].isocalendar().week
            grp_periods.extend(range(start_wk, start_wk + no_wks + 1))

        si_df = pd.DataFrame(list(si))
        rp_df['grp_periods'] = grp_periods
        rp_df.set_index('grp_periods')

        if not si_df.empty:
            recs = si_df.set_index('grp_period').T.to_dict('records')[0]

            # if we are processing months data, convert the datetimes to string before filling in the gaps
            if period_['gid']!= 0 and period_['gid'] < 13:
                recs = {date_.strftime('%d'):no_recs for date_, no_recs in recs.items()}

            rp_df['syndromics'] = [recs[cp] if cp in recs.keys() else 0 for cp in grp_periods]
        
        else: rp_df['syndromics'] = [0]* len(grp_periods)


        
        # notifiable records records per sub county
        if period_['gid'] == 0:
            nddetail = NDDetail.objects.select_related('nd_report').filter(nd_report__datetime_reported__gte=period_['start']).filter(nd_report__datetime_reported__lte=period_['end']).annotate(grp_period=RawSQL('EXTRACT(MONTH FROM datetime_reported)', []) ).values('grp_period').annotate(no_reports=Count('grp_period')).order_by('grp_period')
        elif period_['gid'] < 13:
            nddetail = NDDetail.objects.select_related('nd_report').filter(nd_report__datetime_reported__gte=period_['start']).filter(nd_report__datetime_reported__lte=period_['end']).annotate(grp_period=RawSQL('DATE(datetime_reported)', []) ).values('grp_period').annotate(no_reports=Count('grp_period')).order_by('grp_period')
        else:
            nddetail = NDDetail.objects.select_related('nd_report').filter(nd_report__datetime_reported__gte=period_['start']).filter(nd_report__datetime_reported__lte=period_['end']).annotate(grp_period=RawSQL('EXTRACT(WEEK FROM datetime_reported)', []) ).values('grp_period').annotate(no_reports=Count('grp_period')).order_by('grp_period')

        ndr_df = pd.DataFrame(list( nddetail ))
        if not ndr_df.empty:
            recs = ndr_df.set_index('grp_period').T.to_dict('records')[0]

            # if we are processing months data, convert the datetimes to string before filling in the gaps
            if period_['gid']!= 0 and period_['gid'] < 13:
                recs = {date_.strftime('%d'):no_recs for date_, no_recs in recs.items()}

            rp_df['nd1'] = [recs[cp] if cp in recs.keys() else 0 for cp in grp_periods]
        
        else: rp_df['nd1'] = [0]* len(grp_periods)

        # zero reports per sub county
        if period_['gid'] == 0:
            zeros = NDReport.objects.filter(datetime_reported__gte=period_['start']).filter(datetime_reported__lte=period_['end']).filter(nddetail=None).values('sub_county').annotate(grp_period=RawSQL('EXTRACT(MONTH FROM datetime_reported)', []) ).values('grp_period').annotate(no_reports=Count('grp_period')).order_by('grp_period')
        elif period_['gid'] < 13:
            zeros = NDReport.objects.filter(datetime_reported__gte=period_['start']).filter(datetime_reported__lte=period_['end']).filter(nddetail=None).values('sub_county').annotate(grp_period=RawSQL('DATE(datetime_reported)', []) ).values('grp_period').annotate(no_reports=Count('grp_period')).order_by('grp_period')
        else:
            zeros = NDReport.objects.filter(datetime_reported__gte=period_['start']).filter(datetime_reported__lte=period_['end']).filter(nddetail=None).values('sub_county').annotate(grp_period=RawSQL('EXTRACT(WEEK FROM datetime_reported)', []) ).values('grp_period').annotate(no_reports=Count('grp_period')).order_by('grp_period')

        zeros_df = pd.DataFrame(list( zeros ))
        if not zeros_df.empty:
            recs = zeros_df.set_index('grp_period').T.to_dict('records')[0]
            
            # if we are processing months data, convert the datetimes to string before filling in the gaps
            if period_['gid']!= 0 and period_['gid'] < 13:
                recs = {date_.strftime('%d'):no_recs for date_, no_recs in recs.items()}

            rp_df['zero'] = [recs[cp] if cp in recs.keys() else 0 for cp in grp_periods]
            
        else: rp_df['zero'] = [0]* len(grp_periods)

        rp_df['total'] = rp_df.syndromics + rp_df.nd1 + rp_df.zero
        
        data_cols = ['syndromics', 'nd1', 'zero']
        data_labels = ['Syndromic Records', 'Notifiable Diseases', 'Zero Reports']

        # plot the line graph
        plt.plot(grp_periods, rp_df['total'])
        plt.title('Reporting trend for %s' % period_['period_name'], fontsize=14)
        plt.xlabel(grp_periods_name, fontsize=12)
        plt.ylabel('No of Reports', fontsize=12)
        plt.xlim(auto=True)
        plt.grid(True)

        if settings.USE_S3 == 'True':
            # push it to s3
            client = boto3.client('s3', region_name=settings.AWS_S3_REGION_NAME)
            img_name = '%s.png' % access_code
            img.save(img_name)
            client.upload_file(img_name, settings.AWS_STORAGE_BUCKET_NAME, 'static/qr_codes/%s' % img_name, ExtraArgs={'ACL':'public-read'})
        else:
            plt.savefig(fname="%s/%s" % (settings.STATIC_ROOT, file_name_path))

        plt.close()

    def fetch_graph_cdr_reporters(self, period_, file_name_path):
        rp_df = pd.DataFrame()

        # records per cdr reporter
        sr_df = pd.DataFrame(list( SyndromicIncidences.objects.filter(datetime_reported__gte=period_['start']).filter(datetime_reported__lte=period_['end']).values('reporter').annotate(no_reports=Count('reporter')).order_by('-no_reports') ))

        # the chart will be too long, lets trim it
        if len(sr_df) > settings.MAX_BARS: sr_df = sr_df.truncate(after=25)
        sr_df = sr_df[::-1]     # flip values from top to bottom

        # get the actual names of the reporters
        key_cdrs = sr_df['reporter']
        cdr_df = pd.DataFrame(list(DictionaryItems.objects.filter(t_key__in=key_cdrs).values('t_key', 't_value').distinct('t_key')))
        recs = cdr_df.set_index('t_key').T.to_dict('records')[0]
        sr_df['cdr_names'] = [recs[cdr['reporter']] for index, cdr in sr_df.iterrows()]

        fig, ax = plt.subplots(1, figsize=(12, 8))
        ax.barh(sr_df['cdr_names'], sr_df['no_reports'], 0.35, label='Most Active CDRs')
        ax.set_ylabel('No of Reports')
        ax.set_title('Top 25 CDR Reporters in %s' % period_['period_name'])
        ax.xaxis.grid(color='gray', linestyle='dashed', linewidth=0.5, alpha=0.5)             # show the grid lines
        plt.xlim(auto=True)

        if settings.USE_S3 == 'True':
            # push it to s3
            client = boto3.client('s3', region_name=settings.AWS_S3_REGION_NAME)
            img_name = '%s.png' % access_code
            img.save(img_name)
            client.upload_file(img_name, settings.AWS_STORAGE_BUCKET_NAME, 'static/qr_codes/%s' % img_name, ExtraArgs={'ACL':'public-read'})
        else:
            plt.savefig(fname="%s/%s" % (settings.STATIC_ROOT, file_name_path))

        plt.close(fig)

    def fetch_graph_disease_distibution(self, period_, species, file_name_path):
        sd_df = pd.DataFrame(list( SyndromicDetails.objects.select_related('incidence').filter(incidence__datetime_reported__gte=period_['start']).filter(incidence__datetime_reported__lte=period_['end']).filter(species=species).values('prov_diagnosis').annotate(no_reports=Count('prov_diagnosis')) ))
        
        recs = sd_df.set_index('prov_diagnosis').T.to_dict('records')[0]
        
        # get the keys and split the diseases with multiple options
        new_recs = {}
        for diagnosis, count_ in recs.items():
            if len(diagnosis.split(' ')) == 1:
                new_recs[diagnosis] = count_ if diagnosis not in new_recs else new_recs[diagnosis] + count_
                continue

            # split this and add to our data frame
            for cur_diag in diagnosis.split(' '):
                new_recs[cur_diag] = count_ if cur_diag not in new_recs else new_recs[cur_diag] + count_

        new_recs = pd.DataFrame(list(new_recs.items()), columns=['prov_diagnosis', 'counts'])

        new_recs.sort_values(by=['counts'], inplace=True)
        fig, ax = plt.subplots()
        if len(new_recs) < 4:
            # draw a pie chart
            ax.pie(new_recs['counts'], labels=new_recs['prov_diagnosis'], autopct='%1.1f%%', shadow=False, startangle=90)
            ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

        elif len(new_recs) < 6:
            ax.bar(new_recs['prov_diagnosis'], new_recs['counts'], 0.35, label='Diseases (Prov Diagnosis)')
            ax.set_ylabel('No of Reports')
            ax.set_title('Provisional Diagnosis Distribution for %s' % species)
            ax.xaxis.grid(color='gray', linestyle='dashed', linewidth=0.5, alpha=0.5)             # show the grid lines
            plt.ylim(auto=True)

        else:
            ax.barh(new_recs['prov_diagnosis'], new_recs['counts'], 0.35, label='Diseases (Prov Diagnosis)')
            ax.set_xlabel('No of Reports')
            ax.set_title('Provisional Diagnosis Distribution for %s' % species)
            ax.xaxis.grid(color='gray', linestyle='dashed', linewidth=0.5, alpha=0.5)             # show the grid lines
            plt.xlim(auto=True)


        # we need a custom name for this file
        
        if settings.USE_S3 == 'True':
            # push it to s3
            client = boto3.client('s3', region_name=settings.AWS_S3_REGION_NAME)
            img_name = '%s.png' % access_code
            img.save(img_name)
            client.upload_file(img_name, settings.AWS_STORAGE_BUCKET_NAME, 'static/qr_codes/%s' % img_name, ExtraArgs={'ACL':'public-read'})
        else:
            plt.savefig(fname="%s/%s" % (settings.STATIC_ROOT, file_name_path))

        plt.close(fig)
        print(new_recs)

        # raise Exception('Quit for now')

def generate_report_graphs():
    grapher = GraphsGenerator()

    grapher.determine_graphs_period()
    grapher.generate_graphs()