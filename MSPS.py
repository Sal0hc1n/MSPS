#Sollazzo Nicholas 30/03/2017

import MySQLdb
from operator import itemgetter

'''
My Simple Py SQL
Version: 2
'''

class mspsTools(object):
    """docstring for tools."""
    def __init__(self):
        pass

# return a dictionary
    def fetchOneAssoc(self, cursor):
        data = cursor.fetchone()
        if data is None:
            return None

        desc = cursor.description

        dictionary = {}

        for (name, value) in zip(desc, data):
            dictionary[name[0]] = value

        return dictionary

    def format_as_table(self, data,
                        keys,
                        header=None,
                        sort_by_key=None,
                        sort_order_reverse=False):
        """Takes a list of dictionaries, formats the data, and returns
        the formatted data as a text table.

        Required Parameters:
            data - Data to process (list of dictionaries). (Type: List)
            keys - List of keys in the dictionary. (Type: List)

        Optional Parameters:
            header - The table header. (Type: List)
            sort_by_key - The key to sort by. (Type: String)
            sort_order_reverse - Default sort order is ascending, if
                True sort order will change to descending. (Type: Boolean)

        output example:
            Name         Age Sex
            Bill Clinton 57  M
            John Doe     37  M
            Lisa Simpson 17  F
        """
        # Sort the data if a sort key is specified (default sort order
        # is ascending)
        if sort_by_key:
            data = sorted(data,
                          key=itemgetter(sort_by_key),
                          reverse=sort_order_reverse)

        # If header is not empty, add header to data
        if header:
            header = dict(zip(keys, header))
            data.insert(0, header)

        column_widths = []
        for key in keys:
            column_widths.append(max(len(str(column[key])) for column in data))

        # Create a tuple pair of key and the associated column width for it
        key_width_pair = zip(keys, column_widths)

        format = ('%-*s ' * len(keys)).strip() + '\n'
        formatted_data = ''
        for element in data:
            data_to_format = []
            # Create a tuple that will be used for the formatting in
            # width, value format
            for pair in key_width_pair:
                data_to_format.append(pair[1])
                data_to_format.append(element[pair[0]])
            formatted_data += format % tuple(data_to_format)
        return formatted_data



class msps(object):
    """docstring for msps."""
    def __init__(self, address, user, password, name):
        self.address = address
        self.user = user
        self.password = password
        self.name = name

        # Autenticazione server
        self.db = MySQLdb.connect(address, user, password, name)
        # Creazione cursore per esecuzione query
        self.cursor = self.db.cursor()

        # Creazione oggetto tools
        self.tools = mspsTools()

# chiude la connessione con il server
    def close(self):
        self.db.close()

# esegue una query
    def execute(self, query):
        self.cursor.execute(query)

# esegue una query e controlla che quest'ultima sia andata a buon fine
    def execute_check(self, query, ok = None, err = None):
        try:
           # Esecuzione comando SQL
           self.execute(query)
           # Confermare le modifiche
           self.db.commit()
           if ok is not None:
               print ok
           else:
               print 'committed'

           return True
        except:
           # Rollback in caso di errore
           self.db.rollback()

           if err is not None:
               print err
           else:
                print 'Rollback'

           return False

# esegue una query, controlla che quest'ultima sia andata a buon fine e ritorna il risultato
    def execute_check_fetch(self, query, ok = None, err = None):
        if self.execute_check(query, ok, err):
            result = []

            fetch = self.tools.fetchOneAssoc(self.cursor)

            while fetch is not None:
                result.append( fetch )
                fetch = self.tools.fetchOneAssoc(self.cursor)

            return result # lista di dizionari contenenti tracciato e record

        else:

            return None

 # esegue una query, controlla che quest'ultima sia andata a buon fine e ritorna il risultato
    def execute_check_fetch_table(self, query, ok = None, err = None):

        result = self.execute_check_fetch(query, ok, err)

        if result is not None:
            return self.tools.format_as_table(result, list(result[0].keys()), list(result[0].keys()))
        else:
            return None
