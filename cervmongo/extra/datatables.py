#  datatables.py
#
#  Copyright 2020 Anthony "antcer1213" Cervantes <anthony.cervantes@cerver.info>
#
#  MIT License
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
#
#

# translation for sorting between datatables and mongodb
order_dict = {'asc': 1, 'desc': -1}


class DataTablesServer(object):

    def __init__(self, request, columns, index, db, collection):

        self.columns = columns
        self.index = index
        self.collection = collection
        self.db = db

        # values specified by the datatable for filtering, sorting, paging
        self.request_values = request

        # connection to your mongodb (see pymongo docs).
        # this is defaulted to localhost
        self.dbh = Client("mongodb://{0}:{1}/{2}".format(
                conf['socket_host'],
                conf['socket_port'],
                conf['db']))

        # results from the db
        self.result_data = None

        # total in the table after filtering
        self.cardinality_filtered = 0

        # total in the table unfiltered
        self.cardinality = 0

        self.run_queries()

    def output_result(self):

        try:
            output = {}
            output['sEcho'] = str(int(self.request_values['sEcho']))
            output['iTotalRecords'] = str(self.cardinality)
            output['iTotalDisplayRecords'] = str(self.cardinality_filtered)
            aaData_rows = []

            for row in self.result_data:
                aaData_row = {}
                for i in range(len(self.columns)):
                    value_ = return_value_from_dict(row, self.columns[i])
                    aaData_row[self.columns[i]] = value_
                aaData_rows.append(aaData_row)

            output['aaData'] = aaData_rows

            return output
        except:
            self.dbh.PUT('logs', 'datatable', {'output': _traceback()})
            return []

    def run_queries(self):

        # 'mydb' is the actual name of your database
        mydb = self.dbh[self.db]

        # pages has 'start' and 'length' attributes
        pages = self.paging()

        # the term you entered into the datatable search
        _filter = self.filtering()

        # the document field you chose to sort
        sorting = self.sorting()

        # get result from db to display on the current page
        self.result_data = list(mydb[self.collection].find(filter=_filter,
                                                           skip=pages.start,
                                                           limit=pages.length,
                                                           sort=sorting))

        # length of filtered set
        length_ = len(list(mydb[self.collection].find(filter=_filter)))
        self.cardinality_filtered = length_

        # length of all results you wish to display in the datatable, unfiltered
        self.cardinality = len(list(mydb[self.collection].find()))

    def filtering(self):

        # build your filter spec
        _filter = {}
        check1 = ('sSearch' in self.request_values)
        check2 = (self.request_values['sSearch'] != "")
        if check1 and check2:

            # the term put into search is logically concatenated
            # with 'or' between all columns
            or_filter_on_all_columns = []

            for i in range(len(self.columns)):
                column_filter = {}
                # case insensitive partial string matching pulled from user
                # input
                column_filter[self.columns[i]] = {
                    '$regex': self.request_values['sSearch'],
                    '$options': 'i'
                    }
                or_filter_on_all_columns.append(column_filter)

            _filter['$or'] = or_filter_on_all_columns

        # individual column filtering - uncomment if needed

        #and_filter_individual_columns = []
        #for i in range(len(columns)):
        #    if (request_values.has_key('sSearch_%d' % i) and
        # request_values['sSearch_%d' % i] != ''):
        #        individual_column_filter = {}
        #        individual_column_filter[columns[i]] = {'$regex':
        # request_values['sSearch_%d' % i], '$options': 'i'}
        #        and_filter_individual_columns.append(individual_column_filter)

        #if and_filter_individual_columns:
        #    _filter['$and'] = and_filter_individual_columns

        return _filter

    def sorting(self):

        order = []
        # mongo translation for sorting order
        if ( self.request_values['iSortCol_0'] != "" ) and ( self.request_values['iSortingCols'] > 0 ):

            for i in range( int(self.request_values['iSortingCols']) ):
                # column number
                column_number = int(self.request_values['iSortCol_'+str(i)])
                # sort direction
                sort_direction = self.request_values['sSortDir_'+str(i)]

                order.append((self.columns[column_number], order_dict[sort_direction]))

        return order

    def paging(self):

        pages = namedtuple('pages', ['start', 'length'])

        if (self.request_values['iDisplayStart'] != "" ) and (self.request_values['iDisplayLength'] != -1 ):
            pages.start = int(self.request_values['iDisplayStart'])
            pages.length = int(self.request_values['iDisplayLength'])

        return pages
