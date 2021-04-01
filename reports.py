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
from wordcloud import (WordCloud, get_single_color_func)
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
        cmd_options_ = {
            'margin-top': '10mm',
            'margin-left': '20mm',
            'margin-bottom': '20mm',
            'disable-smart-shrinking': True,
            'footer-right': 'Page [page] of [topage]',
            'footer-font-size': 9,
            'footer-line': True
        }

        report_vars = {
            'report_title': '%s %s' % (report_details['title'], report_details['period_name']),
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
        footer_template = footer_template_
        cmd_options = cmd_options_

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
    graph_names = ['reports', 'scvo_reporters', 'reports_trend', 'cdr_reporters', 'disease_distibution', 'wordcloud', 'n_diseases', 'abattoirs_reporters', 'abattoirs', 'abattoir_lesions', 'drugs_sold', 'drug_sale_location']

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
            # interested in years
            report_period = g_year
            report_details['period_type'] = 'year'
            report_details['report_template'] = 'reports/monthly_report.html'
            report_details['first_date'] = dt.date(g_year + 1, 1, 1)            # for us to get the requested year, ran the reports with Jan-01-NextYear as the requested date
            period_code = 'fy'

        elif g_period < 13:
            # interested in months
            report_period = dt.date(g_year, g_period, 1).strftime("%B")
            report_details['period_type'] = 'Month'
            report_details['report_template'] = 'reports/monthly_report.html'
            if g_period == 12: report_details['first_date'] = dt.date(g_year + 1, 1, 1)
            else: report_details['first_date'] = dt.date(g_year, g_period + 1, 1)
            period_code = 'fm'

        elif g_period < 17:
            # interested in quarters
            report_period = 'Quarter %d,' % (g_period - 12)
            report_details['period_type'] = 'Quarter'
            report_details['report_template'] = 'reports/monthly_report.html'
            if g_period == 16: report_details['first_date'] = dt.date(g_year + 1, 1, 1)
            else: report_details['first_date'] = dt.date(g_year, ((g_period - 12) * 3) - 2, 1)
            period_code = 'fq'

        else:
            report_period = '%d Half of ' % (g_period - 16)
            report_details['period_type'] = 'Half Year'
            report_details['report_template'] = 'reports/monthly_report.html'
            if g_period == 18: report_details['first_date'] = dt.date(g_year + 1, 1, 1)
            else: report_details['first_date'] = dt.date(g_year, ((g_period - 16) * 6) - 5, 1)
            period_code = 'fh'

        report_details['extra_info'] = {}
        # get the period date details
        self.determine_graphs_period(report_details['first_date'])
        period_ = self.t_period[period_code]
        report_details['extras'] = self.report_extra_details(period_)
        
        for r_type in self.graph_names:
            if r_type == 'disease_distibution' or r_type == 'wordcloud':
                # we have multiple graphs here
                all_species = list(SyndromicDetails.objects.select_related('incidence').filter(incidence__datetime_reported__gte=period_['start']).filter(incidence__datetime_reported__lte=period_['end']).values('species').distinct('species'))
                for specie in all_species:
                    report_details['graphs']['%s_%s' % (r_type, specie['species'])] = "%sreports/%s_%s_%s_%d_%d.jpg" % (settings.STATIC_URL, settings.PROJECT_NAME, r_type, specie['species'], g_year, g_period )

                report_details['extra_info']['all_species'] = all_species
            else:
                report_details['graphs'][r_type] = "%s/reports/%s_%s_%d_%d.jpg" % (settings.STATIC_URL, settings.PROJECT_NAME, r_type, g_year, g_period )

        
        report_details['period'] = '%s %s' % (report_period, g_year)
        report_details['period_name'] = report_period


        return report_details

    def report_extra_details(self, period_):
        # get the additional details for this report
        to_return = {}
        
        # total reports received
        sr_df = self.fetch_graph_received_reports(period_, 'dummy_file_path', True)

        to_return['rec_reports_top_county'] = sr_df['sub_county'].iloc[0]
        to_return['rec_reports_top_county_nos'] = int(sr_df['total'].iloc[0])
        to_return['rec_reports_total'] = int(sr_df['total'].sum())

        # trend
        (rp_df, period_name, data_labels) = self.fetch_graph_reports_trend(period_, 'dummy_file_path', True)
        rp_df.sort_values('total', ascending=False, inplace=True)
        
        to_return['high_reports_breakdown'] = '%d %s, %d %s and %d %s - %d total' % (rp_df['syndromics'].iloc[0], data_labels[0], rp_df['nd1'].iloc[0], data_labels[1], rp_df['zero'].iloc[0], data_labels[2], rp_df['total'].iloc[0])
        to_return['low_reports_breakdown'] = '%d %s, %d %s and %d %s - %d total' % (rp_df['syndromics'].iloc[-1], data_labels[0], rp_df['nd1'].iloc[-1], data_labels[1], rp_df['zero'].iloc[-1], data_labels[2], rp_df['total'].iloc[-1])
        
        if period_['gid'] == 0:
            to_return['high_reports_1_period'] = '%s %s' % (dt.date(period_['year'], int(rp_df['grp_periods'].iloc[0]), 1).strftime("%B"), period_['year'])
            to_return['high_reports_2_period'] = '%s %s' % (dt.date(period_['year'], int(rp_df['grp_periods'].iloc[1]), 1).strftime("%B"), period_['year'])
            to_return['high_reports_last_period'] = '%s %s' % (dt.date(period_['year'], int(rp_df['grp_periods'].iloc[-1]), 1).strftime("%B"), period_['year'])

        elif period_['gid'] < 13:
            to_return['high_reports_1_period'] = dt.date(period_['year'], period_['gid'], int(rp_df['grp_periods'].iloc[0])).strftime("%b %d (%A)")
            to_return['high_reports_2_period'] = dt.date(period_['year'], period_['gid'], int(rp_df['grp_periods'].iloc[1])).strftime("%b %d (%A)")
            to_return['high_reports_last_period'] = dt.date(period_['year'], period_['gid'], int(rp_df['grp_periods'].iloc[-1])).strftime("%b %d (%A)")

        else:
            to_return['high_reports_1_period'] = 'Week %d' % rp_df['grp_periods'].iloc[0]
            to_return['high_reports_2_period'] = 'Week %d' % rp_df['grp_periods'].iloc[1]
            to_return['high_reports_last_period'] = 'Week %d' % rp_df['grp_periods'].iloc[-1]

        # CDRs
        sr_df = self.fetch_graph_cdr_reporters(period_, 'dummy_file_path', True)
        to_return['cdr_highest'] = sr_df['cdr_names'][0]
        to_return['cdr_highest_reports'] = sr_df['no_reports'][0]

        # notifiable diseases
        nd_df = self.fetch_graph_n_diseases(period_, 'dummy_file_path', True)
        if nd_df.empty: to_return['nd_sum'] = 0
        else:
            to_return['nd_high1'] = nd_df['disease'][0]
            to_return['nd_high2'] = nd_df['disease'][1]
            to_return['nd_low1'] = nd_df['disease'].iloc[0]
            to_return['nd_low2'] = nd_df['disease'].iloc[1]
            to_return['nd_sum'] = sum(nd_df['no_reports'])

        sh_df, sh_reporters = self.fetch_graph_abattoirs_reporters(period_, 'dummy_file_path', True)
        if sh_df.empty: to_return['sh_sum'] = 0
        else:
            to_return['sh_high_reporter'] = sh_reporters[0]
            to_return['sh_sum'] = sum(sh_df['no_reports'])
            to_return['sh_high_abattoir'] = sh_df['reporter'][0]
            to_return['sh_high_reporter_no'] = sh_df['no_reports'][0]
            to_return['sh_high_reporter_perc'] = "%.1f" % ((sh_df['no_reports'][0] / to_return['sh_sum'])*100)

        sh_df, species_, all_abattoirs = self.fetch_graph_abattoirs_reports(period_, 'dummy_file_path', True)
        if sh_df.empty: to_return['sh_sl_sum'] = 0
        else:
            to_return['sh_sl_sum'] = int(sum(sh_df['total']))
            to_return['sh_sl_high_specie'] = species_[0]
            to_return['sh_sl_high_specie_perc'] = "%.1f" % (( sum(sh_df[to_return['sh_sl_high_specie']]) / sum(sh_df['total']))*100)
            to_return['sh_sl_high_abattoir'] = all_abattoirs[0]
            to_return['sh_sl_high_abattoir_perc'] = "%.1f" % (( sh_df['total'].iloc[0] / sum(sh_df['total']))*100)

        sh_lesions = self.fetch_graph_abattoir_lesions(period_, 'dummy_file_path', True)
        if sh_lesions.empty: to_return['sh_ls_sum'] = 0
        else:
            to_return['sh_ls_sum'] = 100    # put an arbitrary figure to show the data frame is not emty
            to_return['sh_ls_top_lesion'] = sh_lesions['lesion_name'].iloc[0]

        ag_reports = self.fetch_graph_drugs_sold(period_, 'dummy_file_path', True)

        if ag_reports.empty: to_return['ag_reports_received'] = 0
        else:
            agrovets = pd.DataFrame(list( AGReport.objects.filter(datetime_reported__gte=period_['start']).filter(datetime_reported__lte=period_['end']).values('agrovet_name').annotate(no_drugs=Count('agrovet_name')).order_by('-no_drugs') ))

            to_return['ag_reports_received'] = sum(ag_reports['no_drugs'])
            to_return['ag_reports_top_drug'] = ag_reports['drug_sold'][0]
            to_return['ag_reports_no_agrovets'] = agrovets['agrovet_name'].size
            to_return['ag_reports_agrovets'] = ', '.join(list(agrovets['agrovet_name']))
            to_return['ag_reports_top_drugs'] = '%s, %s and %s' % (ag_reports['drug_sold'][0], ag_reports['drug_sold'][1], ag_reports['drug_sold'][2])

        return to_return

    def determine_graphs_period(self, report_date = None):
        print(report_date)
        try:
            # when called, determine the period to use in generating the graphs
            
            if report_date is None: report_date = dt.datetime.now()

            # get the interested complete month
            self.t_period['fm']['year'] = report_date.year if report_date.month > 1 else report_date.year - 1
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
                        print("Generate the '%s' report for %s%s" % (r_type, period_['year'], '' if key_ == 'fy' else '_%s%d' % (key_, period_['no'])) )
                        self.fetch_graph_reports(period_, r_type, file_name_path)

        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry_sdk.capture_exception(e)
            raise Exception('There was an error while generating the graphs')

    def save_graphs(self, plt, file_name_path):
        if settings.USE_S3 == 'True':
            # push it to s3
            client = boto3.client('s3', region_name=settings.AWS_S3_REGION_NAME)
            img_name = '%s.png' % access_code
            img.save(img_name)
            client.upload_file(img_name, settings.AWS_STORAGE_BUCKET_NAME, 'static/qr_codes/%s' % img_name, ExtraArgs={'ACL':'public-read'})
        else:
            plt.savefig(fname="%s/%s" % (settings.STATIC_ROOT, file_name_path))

        plt.close()

    def fetch_graph_reports(self, period_, r_type, file_name_path):
        if r_type == 'reports': self.fetch_graph_received_reports(period_, file_name_path)
        elif r_type == 'scvo_reporters': self.fetch_graph_scvo_reporters(period_, file_name_path)
        elif r_type == 'reports_trend': self.fetch_graph_reports_trend(period_, file_name_path)
        elif r_type == 'cdr_reporters': self.fetch_graph_cdr_reporters(period_, file_name_path)
        elif r_type == 'n_diseases': self.fetch_graph_n_diseases(period_, file_name_path)
        elif r_type == 'abattoirs_reporters': self.fetch_graph_abattoirs_reporters(period_, file_name_path)
        elif r_type == 'abattoirs': self.fetch_graph_abattoirs_reports(period_, file_name_path)
        elif r_type == 'abattoir_lesions': self.fetch_graph_abattoir_lesions(period_, file_name_path)
        elif r_type == 'drugs_sold': self.fetch_graph_drugs_sold(period_, file_name_path)
        elif r_type == 'drug_sale_location': self.fetch_graph_drug_sale_location(period_, file_name_path)
        elif r_type == 'disease_distibution' or r_type == 'wordcloud':
            # get the species in this period
            all_species = list(SyndromicDetails.objects.select_related('incidence').filter(incidence__datetime_reported__gte=period_['start']).filter(incidence__datetime_reported__lte=period_['end']).values('species').distinct('species'))
            
            for specie in all_species:
                file_name_path = "reports/%s_%s_%s_%d_%d.jpg" % (settings.PROJECT_NAME, r_type, specie['species'], period_['year'], period_['gid'] )
                
                if r_type == 'disease_distibution': self.fetch_graph_disease_distibution(period_, specie['species'], file_name_path)
                elif r_type == 'wordcloud': self.fetch_graph_syndromes_wordcloud(period_, specie['species'], file_name_path)

    def fetch_graph_received_reports(self, period_, file_name_path, return_data=False):
        """
            Fetch, analyse and graph data for the received reports
        """
        
        # total reports received
        sr_df = pd.DataFrame(list( SyndromicIncidences.objects.filter(datetime_reported__gte=period_['start']).filter(datetime_reported__lte=period_['end']).values('sub_county').annotate(syndromics=Count('sub_county'))))
        nddetail = pd.DataFrame(list( NDDetail.objects.select_related('nd_report').filter(nd_report__datetime_reported__gte=period_['start']).filter(nd_report__datetime_reported__lte=period_['end']).values('nd_report__sub_county').annotate(nd1=Count('nd_report__sub_county')) ))
        zeros = pd.DataFrame(list( NDReport.objects.filter(datetime_reported__gte=period_['start']).filter(datetime_reported__lte=period_['end']).filter(nddetail=None).values('sub_county').annotate(zero=Count('sub_county')) ))

        empty_df = pd.DataFrame()
        empty_df['syndromics'] = [0] * len(settings.SUB_COUNTIES)
        empty_df['nd1'] = [0] * len(settings.SUB_COUNTIES)
        empty_df['zero'] = [0] * len(settings.SUB_COUNTIES)
        empty_df['sub_county'] = settings.SUB_COUNTIES

        if sr_df.empty: sr_df = empty_df

        if not nddetail.empty: sr_df = sr_df.merge(nddetail, how='outer', left_on='sub_county', right_on='nd_report__sub_county', suffixes=('_synd', '_nd1')).fillna(0)
        else: sr_df = sr_df.merge(empty_df, how='outer', left_on='sub_county', right_on='sub_county', suffixes=('_synd', '_nd1')).fillna(0)

        if not zeros.empty: sr_df = sr_df.merge(zeros, how='outer', left_on='sub_county', right_on='sub_county', suffixes=('', '_zeros')).fillna(0)
        else: sr_df = sr_df.merge(empty_df, how='outer', left_on='sub_county', right_on='sub_county', suffixes=('', '_zeros')).fillna(0)

        sr_df['total'] = sr_df['syndromics'] + sr_df['nd1'] + sr_df['zero']
        sr_df.sort_values('total', ascending=False, inplace=True)

        data_cols = ['syndromics', 'nd1', 'zero']
        data_labels = ['Syndromic Records', 'Notifiable Diseases', 'Zero Reports']
        rp_df = sr_df
        sub_counties_order = list(rp_df.axes[0])

        # calculate the %-ages for the data columns
        for i in data_cols:
            rp_df['{}_perc'.format(i)] = rp_df[i] / rp_df['total']
        
        if return_data: return rp_df

        fig, ax = plt.subplots(1, figsize=(12, 8))
        ax.tick_params(labelsize='medium')

        bottom = len(settings.SUB_COUNTIES) * [0]
        for i, col_ in enumerate(data_cols):
            col = '%s_perc' % col_
            ax.bar(rp_df['sub_county'], rp_df[col_], 0.35, label=data_labels[i], bottom=bottom)
            bottom = bottom + rp_df[col_]

        ax.set_ylabel('No of Records', fontsize=14)
        ax.set_xlabel('Sub Counties', fontsize=14)
        ax.set_title('Sub County Reporting for %s %s %d' % (period_['name'], period_['no'], period_['year']))

        ax.legend(frameon=False, loc='best')      # show the legend
        ax.yaxis.grid(color='gray', linestyle='dashed', linewidth=0.5, alpha=0.5)             # show the grid lines
        plt.xlim(auto=True)
        fig.tight_layout()

        self.save_graphs(plt, file_name_path)

    def fetch_graph_scvo_reporters(self, period_, file_name_path, return_data=False):
        # fetch and graph data on number of reports per sub county vets
        # This report is critical for morale building, but the data is not yet extracted and saved in the database
        print()

    def fetch_graph_reports_trend(self, period_, file_name_path, return_data=False):
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

        if return_data: return rp_df, grp_periods_name, data_labels

        # plot the line graph
        fig, ax = plt.subplots(1, figsize=(12, 8))
        plt.plot(grp_periods, rp_df['total'])
        plt.title('Reporting trend for %s' % period_['period_name'], fontsize=14)
        plt.xlabel(grp_periods_name, fontsize=12)
        plt.ylabel('No of Reports', fontsize=12)
        plt.xlim(auto=True)
        plt.grid(True)

        self.save_graphs(plt, file_name_path)

    def fetch_graph_cdr_reporters(self, period_, file_name_path, return_data=False):
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

        if return_data: return sr_df

        fig, ax = plt.subplots(1, figsize=(12, 8))
        ax.barh(sr_df['cdr_names'], sr_df['no_reports'], 0.35, label='Most Active CDRs')
        ax.set_ylabel('Community Disease Reporters (CDRs)', labelpad=15)
        ax.set_xlabel('No of Reports', labelpad=15)
        ax.set_title('Top 25 CDR Reporters in %s' % period_['period_name'])
        ax.xaxis.grid(color='gray', linestyle='dashed', linewidth=0.5, alpha=0.5)             # show the grid lines
        plt.xlim(auto=True)
        # fig.tight_layout()

        self.save_graphs(plt, file_name_path)

    def fetch_graph_disease_distibution(self, period_, species, file_name_path, return_data=False):
        if os.path.exists("%s/%s" % (settings.STATIC_ROOT, file_name_path)): return
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

        if return_data: return new_recs

        fig, ax = plt.subplots(figsize=(12, 8))
        if len(new_recs) < 4:
            # draw a pie chart
            ax.pie(new_recs['counts'], labels=new_recs['prov_diagnosis'], autopct='%1.1f%%', shadow=False, startangle=90)
            ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

        elif len(new_recs) < 6:
            ax.bar(new_recs['prov_diagnosis'], new_recs['counts'], 0.35, label='Diseases (Prov Diagnosis)')
            ax.set_ylabel('No of Reports')
            ax.set_title('Differential diagnosis for %s in %s' % (species, period_['period_name']))
            ax.xaxis.grid(color='gray', linestyle='dashed', linewidth=0.5, alpha=0.5)             # show the grid lines
            plt.ylim(auto=True)

        else:
            ax.barh(new_recs['prov_diagnosis'], new_recs['counts'], 0.35, label='Diseases (Prov Diagnosis)')
            ax.set_xlabel('No of Reports')
            ax.set_title('Differential diagnosis for %s in %s' % (species, period_['period_name']))
            ax.xaxis.grid(color='gray', linestyle='dashed', linewidth=0.5, alpha=0.5)             # show the grid lines
            plt.xlim(auto=True)

        fig.tight_layout()
        self.save_graphs(plt, file_name_path)

    def fetch_graph_syndromes_wordcloud(self, period_, species, file_name_path, return_data=False):
        if os.path.exists("%s/%s" % (settings.STATIC_ROOT, file_name_path)): return
        sd = SyndromicDetails.objects.select_related('incidence').filter(incidence__datetime_reported__gte=period_['start']).filter(incidence__datetime_reported__lte=period_['end']).filter(species=species).values('clinical_signs')

        all_signs = []
        all_sign_names = []
        for sign_ in sd:
            for sn in sign_['clinical_signs'].split(' '): all_signs.append(sn)
                
        unique_signs = set(all_signs)
        
        signs_df = pd.DataFrame(list(DictionaryItems.objects.filter(t_key__in=unique_signs).values('t_key', 't_value').distinct('t_key')))
        signs_names = signs_df.set_index('t_key').T.to_dict('records')[0]

        for sn in all_signs: all_sign_names.append('"%s"' % signs_names[sn])

        # wc = WordCloud(collocations=False).generate(text.lower())
        wordcloud = WordCloud(width = 800, height = 300, background_color ='white', min_font_size = 10).generate(' '.join(all_sign_names))
  
        # plot the WordCloud image                       
        plt.figure(figsize = (15, 4), facecolor = None)
        plt.imshow(wordcloud)
        plt.axis("off")
        plt.tight_layout(pad = 0)
        
        self.save_graphs(plt, file_name_path)

    def fetch_graph_n_diseases(self, period_, file_name_path, return_data=False):
        nd_df = pd.DataFrame(list( NDDetail.objects.select_related('nd_report').filter(nd_report__datetime_reported__gte=period_['start']).filter(nd_report__datetime_reported__lte=period_['end']).values('disease').annotate(no_reports=Count('disease')).order_by('-no_reports') ))

        nd_df = nd_df[::-1]
        if return_data: return nd_df

        if not nd_df.empty:
            
            fig, ax = plt.subplots(figsize=(12, 8))
            ax.barh(nd_df['disease'], nd_df['no_reports'], 0.35, label='Notifiable Diseases')
            ax.set_xlabel('No of Reports')
            ax.set_title('Reported Notifiable Diseases during %s' % period_['period_name'])
            ax.xaxis.grid(color='gray', linestyle='dashed', linewidth=0.5, alpha=0.5)             # show the grid lines
            plt.xlim(auto=True)
            fig.tight_layout()
        
        else:
            plt.text(0.1, 0.5, 'There were no notifiable diseases reported in %s' % period_['period_name'], fontsize=10, color='red')

        self.save_graphs(plt, file_name_path)

    def fetch_graph_abattoirs_reporters(self, period_, file_name_path, return_data=False):
        sh_reporters = pd.DataFrame(list( SHReport.objects.filter(datetime_reported__gte=period_['start']).filter(datetime_reported__lte=period_['end']).values('reporter').annotate(no_reports=Count('reporter')).order_by('-no_reports') ))

        if sh_reporters.empty:
            if return_data: return pd.DataFrame(), []

            plt.text(0.1, 0.5, 'Slaughter house data was not submitted for %s' % period_['period_name'], fontsize=10, color='red')
        
        else:
            # get the actual names of the reporters
            reporters = sh_reporters['reporter']
            reporters_df = pd.DataFrame(list(DictionaryItems.objects.filter(t_key__in=reporters).values('t_key', 't_value').distinct('t_key')))
            if reporters_df.empty:
                reporter_names = reporters
            else:
                recs = reporters_df.set_index('t_key').T.to_dict('records')[0]
                reporter_names = [recs[reporter['reporter']] for index, reporter in reporters_df.iterrows()]

            if return_data: return sh_reporters, reporter_names

            fig, ax = plt.subplots(1, figsize=(12, 8))
            ax.bar(reporter_names, sh_reporters['no_reports'], 0.35, label='Reporters')
            ax.set_ylabel('No of Reports')
            ax.set_title('Reporting by Slaughter House Reporters in %s' % period_['period_name'])
            ax.yaxis.grid(color='gray', linestyle='dashed', linewidth=0.5, alpha=0.5)             # show the grid lines
            plt.ylim(auto=True)
            fig.tight_layout()
            
        self.save_graphs(plt, file_name_path)

    def fetch_graph_abattoirs_reports(self, period_, file_name_path, return_data=False):
        # get the abattoirs and species
        all_species = pd.DataFrame(list(SHSpecies.objects.select_related('sh_report').filter(sh_report__datetime_reported__gte=period_['start']).filter(sh_report__datetime_reported__lte=period_['end']).values('specie').distinct('specie')))
        all_abattoirs = pd.DataFrame(list(SHReport.objects.values('abattoir').distinct('abattoir')))

        sh_records = pd.DataFrame(list( SHSpecies.objects.select_related('sh_report').filter(sh_report__datetime_reported__gte=period_['start']).filter(sh_report__datetime_reported__lte=period_['end']).values('sh_report__abattoir', 'specie').annotate(no_reports=Sum('no_slaughtered')).order_by('sh_report__abattoir', 'specie') ))

        if sh_records.empty:
            if return_data: return pd.DataFrame(), [], []
            plt.text(0.1, 0.5, 'Slaughter house data was not submitted for %s' % period_['period_name'], fontsize=10, color='red')
        
        else:
            sh_df = pd.pivot_table(sh_records, values='no_reports', columns=['specie'], index=['sh_report__abattoir'], aggfunc=np.sum, fill_value=0, margins=True).sort_values('All', ascending=False).sort_values('All', ascending=False, axis=1).drop('All').drop('All', axis=1)
            species_ = list(sh_df.keys())
            all_abattoirs_ = list(sh_df.index)
            sh_df['total'] = sh_df.sum(axis=1)

            sh_df.sort_values('total', ascending=False, inplace=True)

            # calculate the %-ages for the data columns
            for i in species_: sh_df['{}_perc'.format(i)] = sh_df[i] / sh_df['total']

            if return_data: return sh_df, species_, all_abattoirs_

            fig, ax = plt.subplots(1, figsize=(12, 8))
            bottom = len(all_abattoirs_) * [0]
            for i, col_ in enumerate(species_):
                col = '%s_perc' % col_
                ax.bar(all_abattoirs_, sh_df[col_], 0.35, label=species_[i], bottom=bottom)
                bottom = bottom + sh_df[col_]

            ax.set_xlabel('Slaughter Houses')
            ax.set_ylabel('Number of Slaughtered Animals')
            ax.set_title('Slaughtered Animals in %s' % period_['period_name'])
            
            ax.legend(frameon=False, loc='best', fontsize='small')      # show the legend
            ax.yaxis.grid(color='gray', linestyle='dashed', linewidth=0.5, alpha=0.5)             # show the grid lines
            plt.xlim(auto=True)
            fig.tight_layout()

        self.save_graphs(plt, file_name_path)

    def fetch_graph_abattoir_lesions(self, period_, file_name_path, return_data=False):
        all_species = pd.DataFrame(list(SHSpecies.objects.select_related('sh_report').filter(sh_report__datetime_reported__gte=period_['start']).filter(sh_report__datetime_reported__lte=period_['end']).values('specie').distinct('specie')))
        sh_lesions = pd.DataFrame(list( SHParts.objects.select_related('sh_specie', 'sh_report').filter(sh_specie__sh_report__datetime_reported__gte=period_['start']).filter(sh_specie__sh_report__datetime_reported__lte=period_['end']).values('lesions', 'sh_specie__specie').annotate(no_lesions=Sum('no_condemned')).order_by('lesions', 'sh_specie__specie') ))

        if sh_lesions.empty:
            if return_data: return pd.DataFrame()
            plt.text(0.1, 0.5, 'Slaughter house data was not submitted for %s' % period_['period_name'], fontsize=10, color='red')
        
        else:
            sh_lesions = pd.pivot_table(sh_lesions, values='no_lesions', columns=['sh_specie__specie'], index=['lesions'], aggfunc=np.sum, fill_value=0, margins=True).sort_values('All', ascending=False).sort_values('All', ascending=False, axis=1).drop('All').drop('All', axis=1)
            
            for specie in all_species['specie']:
                if specie not in sh_lesions.keys(): sh_lesions[specie] = len(all_species['sh_specie__specie']) * [0]

            lesions_df = pd.DataFrame(list(DictionaryItems.objects.filter(t_key__in=list(sh_lesions.axes[0])).values('t_key', 't_value').distinct('t_key')))
            recs = lesions_df.set_index('t_key').T.to_dict('records')[0]
            lesion_names = [recs[ls] for ls in list(sh_lesions.axes[0])]
            sh_lesions['lesion_name'] = lesion_names

            if return_data: return sh_lesions

            fig, ax = plt.subplots(1, figsize=(12, 8))
            left_w = np.arange(len(lesion_names))
            bar_width = 0.1
            
            ax = sh_lesions.plot(kind="bar", figsize=(12, 8))
            ax.set_xlabel('Observed Lesions', labelpad=15)
            ax.set_ylabel('No of observed lesions', labelpad=15)
            
            plt.xticks(rotation=0)
            ax.set_xticks(left_w + bar_width)
            ax.set_xticklabels(lesion_names)
            
            ax.legend(frameon=False, loc='best', fontsize='small')      # show the legend
            ax.yaxis.grid(color='gray', linestyle='dashed', linewidth=0.5, alpha=0.5)             # show the grid lines

            ax.set_title('Observed lesions per species', pad=15)
            fig.tight_layout()

        self.save_graphs(plt, file_name_path)

    def fetch_graph_drugs_sold(self, period_, file_name_path, return_data=False):
        ag_df = pd.DataFrame(list( AGDetail.objects.select_related('ag_report').filter(ag_report__datetime_reported__gte=period_['start']).filter(ag_report__datetime_reported__lte=period_['end']).values('drug_sold').annotate(no_drugs=Count('drug_sold')).order_by('-no_drugs') ))

        if ag_df.empty:
            if return_data: return pd.DataFrame()
            plt.text(0.1, 0.5, 'There were no agrovet records submitted for %s' % period_['period_name'], fontsize=10, color='red')
        
        else:
            ag_df = ag_df[::-1]
            if return_data: return ag_df

            fig, ax = plt.subplots(figsize=(12, 8))
            ax.barh(ag_df['drug_sold'], ag_df['no_drugs'], 0.35, label='Drugs')
            ax.set_xlabel('No of drugs sold')
            ax.set_title('Drugs sold during %s' % period_['period_name'])
            ax.xaxis.grid(color='gray', linestyle='dashed', linewidth=0.5, alpha=0.5)             # show the grid lines
            plt.xlim(auto=True)
            fig.tight_layout()

        self.save_graphs(plt, file_name_path)

    def fetch_graph_drug_sale_location(self, period_, file_name_path, return_data=False):
        ag_df = pd.DataFrame(list( AGDetail.objects.select_related('ag_report').filter(ag_report__datetime_reported__gte=period_['start']).filter(ag_report__datetime_reported__lte=period_['end']).values('drug_sold', 'farmer_location').annotate(no_drugs=Count('drug_sold')).order_by('drug_sold', 'farmer_location') ))

        if ag_df.empty:
            plt.text(0.1, 0.5, 'There were no agrovet records submitted for %s' % period_['period_name'], fontsize=10, color='red')
        
        else:
            ag_df = pd.pivot_table(ag_df, values='no_drugs', columns=['drug_sold'], index=['farmer_location'], aggfunc=np.sum, fill_value=0, margins=True).sort_values('All', ascending=False).sort_values('All', ascending=False, axis=1).drop('All').drop('All', axis=1)
            drugs_sold = ag_df.to_numpy()
            drugs_list = list(ag_df.axes[1])
            
            locations_df = pd.DataFrame(list(DictionaryItems.objects.filter(t_key__in=list(ag_df.axes[0])).values('t_key', 't_value').distinct('t_key')))
            recs = locations_df.set_index('t_key').T.to_dict('records')[0]
            locations_names = [recs[lns] for lns in list(ag_df.axes[0])]

            if return_data: return drugs_sold, locations_names, drugs_sold

            fig, ax = plt.subplots(figsize=(9, 12))
            im = ax.imshow(drugs_sold)

            # We want to show all ticks...
            ax.set_xticks(np.arange(len(drugs_list)))
            ax.set_yticks(np.arange(len(locations_names)))
            # ... and label them with the respective list entries
            ax.set_xticklabels(drugs_list)
            ax.set_yticklabels(locations_names)

            # Rotate the tick labels and set their alignment.
            plt.setp(ax.get_xticklabels(), rotation=90, ha="right", rotation_mode="anchor")

            # Loop over data dimensions and create text annotations.
            for i in range(len(locations_names)):
                for j in range(len(drugs_list)):
                    text = ax.text(j, i, drugs_sold[i, j], ha="center", va="center", color="w")

            ax.set_title("Drug sales per location during %s" % period_['period_name'])
            fig.tight_layout()

        self.save_graphs(plt, file_name_path)


def generate_report_graphs():
    grapher = GraphsGenerator()

    grapher.determine_graphs_period()
    grapher.generate_graphs()