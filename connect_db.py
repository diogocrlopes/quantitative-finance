import os
import sqlite3
import string as st
import datetime
import names
import csv
import gen_random_values

class Connect(object):

    def __init__(self,db_name):
        try:
            #connecting...
            self.conn = sqlite3.connect(db_name)
            self.cursor = self.conn.cursor()

            #print nome do banco de dados
            print("Banco:", db_name)
            # lendo a versão do SQLite
            self.cursor.execute('SELECT SQLITE_VERSION()')
            self.data = self.cursor.fetchone()
            # imprimindo a versão do SQLite
            print("SQLite version: %s" % self.data)
        except sqlite3.Error:
            print("Erro ao abrir banco.")
            return False

    def commit_db(self):
        if self.conn:
            self.conn.commit()

    def close_db(self):
        if self.conn:
            self.conn.close()
            print("Conexão fechada.")

class Database(object):


    def __init__(self, db_name):
        self.db = Connect(db_name)

    def fechar_conexao(self):
        self.db.close_db()

    def ler_todas_empresas(self):
        sql = 'SELECT * FROM Info_gerais ORDER BY Ticker'
        r = self.db.cursor.execute(sql)
        return r.fetchall()

    def imprimir_todos_clientes(self):
        lista = self.ler_todas_empresas()
        for c in lista:
            print(c)

    def localiza_tabela(self, tabela, nome_tabela = "Info_gerais"):
        #Verifica na tabela se existe o parâmetro passado

        query = 'SELECT * FROM {} '.format(tabela)
        r = self.db.cursor.execute(
                 query)
        if r.fetchone() != None:
            print("Empresa localizada com sucesso ")
            print(r.fetchone())
        else:
            print("Empresa não encontrada")
        return r.fetchone()

    def localiza_empresa(self, ticker):
        #TO DO: Verificar se tem classe repetida e avisar onde está
        query = 'SELECT * FROM Info_gerais WHERE Ticker = ?'
        columnValues = (ticker,)

        r = self.db.cursor.execute(
            query, columnValues)

        records = r.fetchmany(1)
        if records == None:
            print("Empresa não encontrada")
        else:
            print("Empresa localizada com sucesso ")
            print(records)
        return records

    def update(self, id, Ticker, Market, Marketcap, Beta, Country, Sector, Industry ):
        try:
            dado = self.localiza_tabela(id)
            if dado:
                if id == "Info_gerais":
                    try:
                        query = "UPDATE Info_gerais " \
                                "SET  Market = ?, Marketcap = ?, Beta = ?, Country = ?, Sector = ?, Industry = ?" \
                                "WHERE Ticker = ?"
                        columnValues = (Market, Marketcap, Beta, Country, Sector, Industry, Ticker)
                        self.db.cursor.execute(
                            query,columnValues
                        )
                        self.db.commit_db()
                        self.db.cursor.close()

                    except sqlite3.Error as error:
                        print("Ocorreu o seguinte erro ao tentar fazer update do BD", error)

                    finally:
                        self.db.close_db()
            else:
                pass
        except:
            pass

    def inserir_varios_registros(self):
        self.db.cursor.execute('select * from Info_gerais')
        self.r = self.db.cursor.fetchone()
        print(self.r)

    def inserir_de_csv(self):
        pass

    def listar_tabelas(self): #retirar os parênteses e a vírgula?
        self.db.cursor.execute("""
        SELECT name FROM sqlite_master WHERE type='table' ORDER BY name
        """)
        self.names = []
        print('Tabelas:')
        for tabela in self.db.cursor.fetchall():
            print("%s" % (tabela))
            self.names.append(tabela)

        return self.names

    def schema_tabela(self, name): #como faz para pegar as colunas daqui?
        # obtendo o schema da tabela
        self.db.cursor.execute("""
        SELECT sql FROM sqlite_master WHERE type='table' AND name=?
        """, (name,))

        print('Schema:')
        for schema in self.db.cursor.fetchall():
            print("%s" % (schema))

    def visualizar_colunas(self, nome_tabela):
        # obtendo informações da tabela
        self.db.cursor.execute('PRAGMA table_info({})'.format(nome_tabela))

        colunas = [tupla[1] for tupla in self.db.cursor.fetchall()]

        print('Colunas:', colunas)

        return colunas

if __name__ == '__main__':
    c = Database('Companies.db')
    lista = c.listar_tabelas()
    print(lista)
    columns = c.visualizar_colunas('Income_Quartely')
    #c.localiza_tabela('Info_gerais')

    c.localiza_empresa('CIEN')
    #c.update('Info_gerais','CIEN' ,'Brazil', 10000, 'Beta', 'Brazil', 'Petroleum', ' Ohoy' )
    #c.inserir_varios_registros()
    print(len(columns))

