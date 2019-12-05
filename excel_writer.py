from pyexcelerate import Workbook
import logging, traceback, json
from .terminal_output import Terminal

terminal = Terminal()

class ExcelWriter():

    def __init__(self, workbook_name):
        self.wb_name = workbook_name
        self.pending_processing = {}
        self.pending_processing_tmp = {}
        self.sorted_sheet_fields = {}

    def create_workbook(self, data, structure):
        # given the data as json and the structure as json too, create a workbook with this data

        self.wb = Workbook()
        # order the fields for proper display
        for sheet_name, sheet_fields in list(structure.items()):
            self.sorted_sheet_fields[sheet_name] = self.order_fields(sheet_fields)

        self.pending_processing['main'] = {'data':data, 'is_processed': False}

        # from the docs: https://docs.python.org/2.7/tutorial/datastructures.html#dictionaries
        # It is sometimes tempting to change a list while you are looping over it;
        # however, it is often simpler and safer to create a new list instead
        # so lets not change the list while iterating
        while len(list(self.pending_processing.keys())) != 0:
            all_processed = True
            for sheet_name, data in list(self.pending_processing.items()):
                if data['is_processed'] == False:
                    terminal.tprint('Processing '+ sheet_name, 'okblue')
                    all_processed = False
                    self.process_and_write(data['data'], sheet_name)
                    # mark it as processed
                    self.pending_processing[sheet_name]['is_processed'] = True
                    break

            if all_processed == True:
                break

        self.wb.save(self.wb_name)
        return False

    def process_and_write(self, data, sheet_name):
        # contains 2D array of all the data for the current sheet
        cur_records = []
        cur_records.append(self.sorted_sheet_fields[sheet_name])

        for record in data:
            # contains 1D array of data for the current record data
            this_record = []
            for field in self.sorted_sheet_fields[sheet_name]:
                try:
                    cur_value = record[field]
                except KeyError as e:
                    cur_value = '-'

                if isinstance(cur_value, list) == True:
                    # defer processing
                    if field not in self.pending_processing:
                        terminal.tprint("\tFound new sheet (%s) data to save. Deferring for now." % field, 'warn')
                        self.pending_processing[field] = {'data':[], 'is_processed': False}

                    self.pending_processing[field]['data'].extend(cur_value)
                    # add a link
                    this_record.append('Check '+field)
                else:
                    this_record.append(cur_value)

            cur_records.append(this_record)

        # now lets do a batch write of our data
        terminal.tprint("\tBatch writing of "+ sheet_name, 'ok')
        self.wb.new_sheet(sheet_name, data = cur_records)


    def order_fields(self, fields):
        fields.sort()

        # remove the parent_id if its there and append it at the begining
        if 'parent_id' in fields:
            fields.remove('parent_id')
            fields.insert(0, 'parent_id')

        if 'top_id' in fields:
            fields.remove('top_id')
            fields.insert(0, 'top_id')

        fields.remove('unique_id')
        fields.insert(0, 'unique_id')

        return fields