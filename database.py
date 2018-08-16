import re

import mysql.connector

class Attribute:
  def __init__(self, name, type):
    self.name = name
    self.type = type      # text, number

class Relation:
  def __init__(self, name, attrs):
    self.name = name
    self.attrs = attrs

def convert_mysql_type(mysql_type):
  if mysql_type.startswith('int') or mysql_type.endswith('int') or mysql_type.startswith('float') or mysql_type.startswith('double') or mysql_type.startswith('decimal') or mysql_type.startswith('numeric'):
    return 'num'
  elif mysql_type == 'text' or mysql_type.startswith('varchar') or mysql_type.startswith('char') or mysql_type.startswith('enum'):
    return 'text'
  else:
    return mysql_type

class Database:
  # relations to ignore from db
  IGNORE_RELS = ['size', 'history']

  def __init__(self, user, pw, host, db):
    self.conn = mysql.connector.connect(user=user, password=pw, host=host, database=db)
    self.name = db
    self.load_relations()

  def load_relations(self):
    self.relations = {}

    # get relation names
    cursor = self.conn.cursor()
    cursor.execute('SHOW TABLES')
    rel_names = []
    for r in cursor:
      if r[0] not in self.IGNORE_RELS:
        rel_names.append(r[0])
    cursor.close()

    # get attributes for each relation, generate Relations
    for rel_name in rel_names:
      cursor = self.conn.cursor()
      cursor.execute('SHOW COLUMNS FROM {}'.format(rel_name))
      attrs = {}
      for r in cursor:
        attr_name = r[0]
        attr_type = convert_mysql_type(r[1])
        attrs[attr_name] = Attribute(attr_name, attr_type)
      cursor.close()

      self.relations[rel_name] = Relation(rel_name, attrs)

  def cursor(self):
    return self.conn.cursor()

  def set_timeout(self, timeout):
    cursor = self.cursor()
    cursor.execute('SET SESSION MAX_EXECUTION_TIME={}'.format(timeout))
    cursor.close()

  # given a fragment from a SQL statement (e.g. 'actor_0.name')
  # find the type of the attr
  def get_attr_type(self, frag):
    rel_alias, attr_name = frag.split('.')
    m = re.match('([A-Za-z_]+)_[0-9]+', rel_alias)
    if m:
      rel_name = m.group(1)
      return self.relations[rel_name].attrs[attr_name].type
    else:
      raise Exception('Attribute not found for [{}].'.format(frag))
