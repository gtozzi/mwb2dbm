
'''
Database Objects

@author Gabriele Tozzi <gabriele@tozzi.eu>
@brief this file is part of mysqlworkbench2pgmodeler
'''

import re
import collections


MAX_NAME_LEN = 63
VIEW_CLEAN_REGEX = r"(CREATE VIEW [\u0080-\uFFFF]+ AS)|(CREATE VIEW [`\"]+[0-9,a-z,A-Z$_]+[`\"]+ AS)"

class Color:
	''' And RGB color '''

	def __init__(self, str):
		''' Init from string representation #ffffff '''
		assert len(str) == 7
		assert str[0] == '#'

		self.r = int(str[1:3], 16)
		self.g = int(str[3:5], 16)
		self.b = int(str[5:7], 16)

	def add(self, val):
		''' Add value to all r/g/b vals '''
		self.r = max(min(self.r + val, 255), 0)
		self.g = max(min(self.g + val, 255), 0)
		self.b = max(min(self.b + val, 255), 0)

	def __str__(self):
		return '#{:02X}{:02X}{:02X}'.format(self.r, self.g, self.b)


class BaseObjFromEl:

	def __init__(self, el):
		self.id = el.get('id')

		self.attrs = collections.OrderedDict()

		for child in el:
			if child.tag not in {'value', 'link'}:
				continue
			if 'key' not in child.keys():
				continue
			if 'type' not in child.keys():
				continue

			key = child.get('key')
			type = child.get('type')
			value = child.text
			if value == '':
				value = None

			assert key not in self.attrs, key

			if type == 'string':
				pass
			elif type == 'int':
				value = int(value)
			elif type == 'real':
				value = float(value)
			elif type == 'list':
				#TODO
				value = []
			elif type == 'dict':
				#TODO
				value = {}
			elif type == 'object':
				# Should be the ID of the linked object
				pass
			else:
				raise NotImplementedError('Unknown type "{}": "{}"'.format(type, value))

			self.attrs[key] = value

	def __contains__(self, key):
		return key in self.attrs

	def __getitem__(self, key):
		return self.attrs[key]

	def __setitem__(self, key, val):
		self.attrs[key] = val

	def __len__(self):
		return len(self.attrs)

	def __repr__(self):
		return '<{} {}>'.format(
			self.__class__.__name__,
			", ".join(["{}: {}".format(k,v) for k, v in self.attrs.items()])
		)


class DataType:

	TYPE_RE = re.compile('^com.mysql.rdbms.mysql.datatype.([a-z_]+)$')

	def __init__(self, id, type):
		m = self.TYPE_RE.match(type)
		assert m, type

		self.id = id
		self.nativeType = type
		self.type = m.group(1).upper()



class SimpleType(DataType):

	def __init__(self, el):
		assert el.tag == 'link', el.attrib

		super().__init__(el.text, el.text)

	def __repr__(self):
		return "<SimpleType {}>".format(self.type)


class UserType(BaseObjFromEl, DataType):

	def __init__(self, el):
		assert el.tag == 'value', el.attrib

		BaseObjFromEl.__init__(self, el)

		st = el.find("./link[@key='actualType']")
		assert st is not None, list(el)

		DataType.__init__(self, el.get('id'), st.text)


class Column(BaseObjFromEl):

	TYPE_INT = 'com.mysql.rdbms.mysql.datatype.int'

	def __init__(self, el, types):
		super().__init__(el)

		flags = el.find("./value[@key='flags']")

		self['flags'] = []
		for flag in flags:
			self['flags'].append(flag.text)

		# Every col MUST have a simpleType or an userType
		userType = el.find("./link[@key='userType']")
		simpleType = el.find("./link[@key='simpleType']")
		assert userType is not None or simpleType is not None, el.attrib
		assert not (userType is not None and simpleType is not None), el.attrib

		typeId = userType.text if userType is not None else simpleType.text
		self.type = types[typeId]

		# Assigned when this is part of a ForeignKey
		self.fk = None

		# Assigned when this is part of some Index
		self.indices = []



class IndexColumn(BaseObjFromEl):

	def __init__(self, el, index, tableCols):
		super().__init__(el)

		self.index = index

		for tcol in tableCols:
			if tcol.id == self['referencedColumn']:
				break
		else:
			raise RuntimeError('Corresponding table column not found {}'.format(self))

		assert self not in tcol.indices, tcol
		tcol.indices.append(self)
		self.tableCol = tcol


class Index(BaseObjFromEl):

	TYPE_PRIMARY = 'PRIMARY'
	TYPE_UNIQUE = 'UNIQUE'
	TYPE_INDEX = 'INDEX'

	TYPES = { TYPE_PRIMARY, TYPE_UNIQUE, TYPE_INDEX }

	def __init__(self, el, tableCols):
		super().__init__(el)

		assert self['indexType'] in self.TYPES, self
		assert self['isPrimary'] == (self['indexType'] == self.TYPE_PRIMARY), self

		columns = el.find("./value[@key='columns']")
		assert len(columns), list(el)

		self.columns = []
		for column in columns:
			self.columns.append(IndexColumn(column, self, tableCols))


class ForeignKey(BaseObjFromEl):

	def __init__(self, el, table):
		super().__init__(el)

		self.primary = False
		self.table = table

		columns = el.find("./value[@key='columns']")
		self.columns = []
		for column in columns:
			for col in self.table.columns:
				if col.id == column.text:
					assert col.fk is None, col
					col.fk = self
					self.columns.append(col)
					break
			else:
				raise RuntimeError("Column {} not found".format(column.text))

			for idx in col.indices:
				if idx.index['indexType'] == Index.TYPE_PRIMARY:
					self.primary = True

class Trigger(BaseObjFromEl):

	def __init__(self, el, table):
		super().__init__(el)

		self.table = table
		self.timing = el.find("./value[@key='timing']").text
		self.event = el.find("./value[@key='event']").text
		self.name = el.find("./value[@key='name']").text
		#self.procedureCode = el.find("./value[@key='sqlDefinition']").text

class Table(BaseObjFromEl):
	def __init__(self, el, types):
		super().__init__(el)

		columns = el.find("./value[@key='columns']")
		assert len(columns), list(el)

		self.columns = []
		for column in columns:
			self.columns.append(Column(column, types))

		indices = el.find("./value[@key='indices']")
		assert len(indices), list(el)

		self.indices = []
		for index in indices:
			self.indices.append(Index(index, self.columns))

		fks = el.find("./value[@key='foreignKeys']")

		self.fks = []
		for fk in fks:
			self.fks.append(ForeignKey(fk, self))

		triggers = el.find("./value[@key='triggers']")

		self.triggers = []
		for trigger in triggers:
			self.triggers.append(Trigger(trigger, self))

class View(BaseObjFromEl):
	def __init__(self, el):
		super().__init__(el)

		self.name = el.find("./value[@key='name']").text
		self.comment = el.find("./value[@key='comment']").text

		# Remove "CREATE VIEW ... AS" from definition
		dirtydef = el.find("./value[@key='sqlDefinition']").text
		self.definition = re.sub(VIEW_CLEAN_REGEX, "", dirtydef, 0, re.MULTILINE)

class Figure(BaseObjFromEl):

	TABLE_TYPE = 'workbench.physical.TableFigure'
	VIEW_TYPE = 'workbench.physical.ViewFigure'

	def __init__(self, el):
		super().__init__(el)

		self.type = el.get('struct-name')


class Layer(BaseObjFromEl):
	pass


class Diagram(BaseObjFromEl):
	def __init__(self, el):
		super().__init__(el)

		connections = el.find("./value[@key='connections']")
		assert connections is not None, list(el)

		figures = el.find("./value[@key='figures']")
		assert figures is not None, list(el)

		layers = el.find("./value[@key='layers']")
		assert layers is not None, list(el)

		self.figures = []
		for figure in figures:
			self.figures.append(Figure(figure))

		self.layers = []
		for layer in layers:
			self.layers.append(Layer(layer))

	def getTableFigure(self, table):
		for figure in self.figures:
			if figure.type == Figure.TABLE_TYPE and figure['table'] == table.id:
				return figure

		raise KeyError()

	def getViewFigure(self, view):
		for figure in self.figures:
			if figure.type == Figure.VIEW_TYPE and figure['view'] == view.id:
				return figure

		raise KeyError()

	def getFigureLayer(self, figure):
		''' Returns the figure layer, or None if no layer is found '''
		for layer in self.layers:
			if layer.id == figure['layer']:
				return layer

		return None

	def getFirstTableFigureForLayer(self, layer):
		for figure in self.figures:
			if figure.type != Figure.TABLE_TYPE:
				continue

			if self.getFigureLayer(figure) == layer:
				return figure

		raise KeyError()
