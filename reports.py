import os
import json
import logging
import traceback
import sentry_sdk
import datetime as dt
import numpy as np
import pandas as pd
import boto3
import matplotlib.pyplot as plt

from dateutil.relativedelta import relativedelta
from django.db import transaction
from django.db.models import Q, Sum, Count, IntegerField, Min, Max, Avg, F
from django.conf import settings
from django.utils import timezone
from django.views.generic import TemplateView

from wkhtmltopdf.views import PDFTemplateResponse, PDFTemplateView
from hashids import Hashids
from botocore.errorfactory import ClientError

from .models import SyndromicIncidences, SyndromicDetails, NDReport, NDDetail, SHReport, SHSpecies, SHParts, AGReport, AGDetail

from .terminal_output import Terminal

terminal = Terminal()
sentry_sdk.init(settings.SENTRY_DSN, environment=settings.ENV_ROLE)
my_hashids = Hashids(min_length=5, salt=settings.SECRET_KEY)


def report_wrapper(request, hashid):
    try:
        grapher = GraphsGenerator()
        report_details = grapher.report_details(hashid)
        
        filename_ = '%s for %s.pdf' % (report_details['title'], report_details['period'])
        template_name_ = 'reports/monthly_report.html'
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
    graph_names = ['reports']

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

        if g_period == 0:
            report_period = 'Year'
            report_details['period_type'] = 'year'

        elif g_period < 13:
            report_period = dt.date(g_year, g_period, 1).strftime("%B")
            report_details['period_type'] = 'Month'

        elif g_period < 17:
            report_period = 'Quarter %d,' % (g_period - 12)
            report_details['period_type'] = 'Quarter'

        else:
            report_period = '%d Half of ' % (g_period - 16)
            report_details['period_type'] = 'Half Year'
        

        for r_type in self.graph_names:
            report_details['graphs'][r_type] = "%s/reports/%s_%s_%d_%d.jpg" % (settings.STATIC_URL, settings.PROJECT_NAME, r_type, g_year, g_period )

        report_details['period'] = '%s %s' % (report_period, g_year)
        report_details['period_name'] = report_period


        return report_details

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

            # get the interested complete quarter
            curr_quarter = 1+(report_date.month-1)//3
            self.t_period['fq']['year'] = report_date.year if curr_quarter > 1 else report_date.year - 1
            self.t_period['fq']['no'] = 4 if curr_quarter == 1 else curr_quarter - 1
            self.t_period['fq']['start'] = dt.date(self.t_period['fq']['year'], 1+3*(self.t_period['fq']['no']-1), 1)
            self.t_period['fq']['end'] = self.t_period['fq']['start'] + relativedelta(months=3, days=-1)
            self.t_period['fq']['gid'] = self.t_period['fq']['no'] + 12

            # get the interested complete half year
            curr_half = 1+(report_date.month-1)//6
            self.t_period['fh']['year'] = report_date.year if curr_half > 1 else report_date.year - 1
            self.t_period['fh']['no'] = 2 if curr_half == 1 else curr_half - 1
            self.t_period['fh']['start'] = dt.date(self.t_period['fh']['year'], 1+6*(self.t_period['fh']['no']-1), 1)
            self.t_period['fh']['end'] = self.t_period['fh']['start']+relativedelta(months=6, days=-1)
            self.t_period['fh']['gid'] = self.t_period['fh']['no'] + 16

            # get the previous year
            self.t_period['fy']['year'] = report_date.year - 1
            self.t_period['fy']['no'] = ''
            self.t_period['fy']['start'] = dt.date(self.t_period['fy']['year'], 1, 1)
            self.t_period['fy']['end'] = dt.date(self.t_period['fy']['year'], 12, 31)
            self.t_period['fy']['gid'] = 0

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
                        self.fetch_graph_reports(period_, file_name_path)

        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry_sdk.capture_exception(e)
            raise Exception('There was an error while generating the graphs')

    def fetch_graph_reports(self, period_, file_name_path):
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


def generate_report_graphs():
    grapher = GraphsGenerator()

    grapher.determine_graphs_period()
    grapher.generate_graphs()