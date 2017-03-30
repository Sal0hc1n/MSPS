#Sollazzo Nicholas 30/03/2017

from MSPS import msps

'''
My Simple Py SQL
Version: 2
'''

# Indirizzo del server
addr = 'SERVER_ADDRESS'
# Username
usr = 'USERNAME'
# Password
psw = 'P4SSW0RD'
# database
dat = 'DATABASE'

myDB = msps(addr, usr, psw, dat)

print '|OUT| Autenticazione a {} avvenuta. Welcome {}'.format(dat, usr)

myDB.execute_check("DROP TABLE IF EXISTS Dipendente;")


# Creazione di una relazione
sql = """CREATE TABLE Dipendente (
         ID CHAR(2) PRIMARY KEY,
         FIRST_NAME VARCHAR(20) NOT NULL,
         LAST_NAME  VARCHAR(20),
         AGE INT,
         SEX CHAR(1),
         INCOME FLOAT);"""

ok  = '|OUT| Relazione Dipendente correttamente creata'
err = '|ERR| Warning: Rollback'

myDB.execute_check(sql, ok, err)


# SQL INSERT query per inserimento dati nel database
sql = """INSERT INTO Dipendente(ID, FIRST_NAME,
         LAST_NAME, AGE, SEX, INCOME)
         VALUES ('4B','Alessandro', 'Rossi', 18, 'M', 20000);"""

ok  = '|OUT| {} aggiunto ad Dipendente'.format("Alessandro Rossi")
err = '|ERR| Warning: Rollback 1'

myDB.execute_check(sql, ok, err)


# SQL INSERT query per inserimento dati nel database
sql = """INSERT INTO Dipendente(ID, FIRST_NAME,
         LAST_NAME, AGE, SEX, INCOME)
         VALUES ('3A','Cristian', 'Verdi', 19, 'F', 2000);"""

ok  = '|OUT| {} aggiunto ad Dipendente'.format("Cristian Verdi")
err = '|ERR| Warning: Rollback 2'

myDB.execute_check(sql, ok, err)


# SQL INSERT query per inserimento dati nel database
sql = """INSERT INTO Dipendente(ID, FIRST_NAME,
         LAST_NAME, AGE, SEX, INCOME)
         VALUES ('5S','Nicholas', 'Sollazzo', 18, 'M', 50000);"""


ok  = '|OUT| {} aggiunto ad Dipendente'.format("Nicholas Sollazzo")
err = '|ERR| Warning: Rollback 3'

myDB.execute_check(sql, ok, err)


sql = """SELECT * FROM Dipendente;"""

err = '|ERR| Warning: Rollback 4'

print myDB.execute_check_fetch_table(sql, None, err)


sql = """SELECT * FROM Dipendente WHERE SEX='F';"""

err = '|ERR| Warning: Rollback 5'

print myDB.execute_check_fetch_table(sql, None, err)


sql = """SELECT FIRST_NAME, INCOME FROM Dipendente WHERE INCOME>2000;"""

err = '|ERR| Warning: Rollback 6'

print myDB.execute_check_fetch_table(sql, None, err)


# Disconnessione dal server
myDB.close()
