#!/usr/bin/env python3

import re
import copy
import logging
import zipfile
import collections
import lxml.etree


class InvalidFileFormatException(RuntimeError):
	pass


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


class Table(BaseObjFromEl):
	def __init__(self, el, types):
		super().__init__(el)

		columns = el.find("./value[@key='columns']")
		assert len(columns), list(el)

		self.columns = []
		for column in columns:
			self.columns.append(Column(column, types))


class Figure(BaseObjFromEl):

	TABLE_TYPE = 'workbench.physical.TableFigure'

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

	def getFigureLayer(self, figure):
		for layer in self.layers:
			if layer.id == figure['layer']:
				return layer

		raise KeyError()

	def getFirstTableFigureForLayer(self, layer):
		for figure in self.figures:
			if figure.type != Figure.TABLE_TYPE:
				continue

			if self.getFigureLayer(figure) == layer:
				return figure

		raise KeyError()



class Main:
	''' Convert from MySQL Workbench to pgModeler '''

	MWB_INNER_FILE = 'document.mwb.xml'

	def __init__(self):
		self.log = logging.getLogger('main')

	def _addDomainNodes(self, parent, name, baseType, constraintName, constraintExpr):
		domainNode = lxml.etree.Element('domain', {
			'name': name,
			'not-null': 'false',
		})
		parent.append(domainNode)

		domainNode.append(lxml.etree.Element('schema', {
			'name': 'public',
		}))

		domainNode.append(lxml.etree.Element('role', {
			'name': 'postgres',
		}))

		domainNode.append(lxml.etree.Element('type', {
			'name': baseType,
			'length': '0',
		}))

		constraintNode = lxml.etree.Element('constraint', {
			'name': constraintName,
			'type': 'check',
		})
		domainNode.append(constraintNode)

		exprNode = lxml.etree.Element('expression')
		exprNode.text = constraintExpr
		constraintNode.append(exprNode)

	def createDbm(self, dbname, tables, diagram):
		''' Creates a new empty DBM '''
		enumid = 1
		domains = set()

		tree = lxml.etree.ElementTree(lxml.etree.XML('''
			<dbmodel pgmodeler-ver="0.9.2" last-position="0,0" last-zoom="1" max-obj-count="4" default-schema="public" default-owner="postgres">
			</dbmodel>
		'''))
		root = tree.getroot()

		database = lxml.etree.Element('database', {
			'name': dbname,
			'is-template': "false",
			'allow-conns': "true",
		})
		root.append(database)

		schema = lxml.etree.Element('schema', {
			'name': "public",
			'layer': "0",
			'fill-color': "#e1e1e1",
			'sql-disabled': "true",
		})
		root.append(schema)

		# Create layers
		for layer in diagram.layers:
			firstTable = diagram.getFirstTableFigureForLayer(layer)
			color = Color(firstTable['color'])
			bcolor = copy.copy(color)
			bcolor.add(-40)

			# Create text box
			tnode = lxml.etree.Element('textbox', {
				'name': layer['name'],
				'layer': '0',
				'font-size': "9",
			})
			root.append(tnode)

			pnode = lxml.etree.Element('position', {
				'x': str(layer['left']),
				'y': str(layer['top']),
			})
			tnode.append(pnode)

			cnode = lxml.etree.Element('comment')
			cnode.text = layer['name']
			tnode.append(cnode)

			# Create tag
			tnode = lxml.etree.Element('tag', {
				'name': layer['name'].lower(),
			})
			root.append(tnode)

			node = lxml.etree.Element('style', {
				'id': 'table-body',
				'colors': '#fcfcfc,#fcfcfc,#808080',
			})
			tnode.append(node)

			node = lxml.etree.Element('style', {
				'id': 'table-ext-body',
				'colors': '#fcfcfc,#fcfcfc,#808080',
			})
			tnode.append(node)

			node = lxml.etree.Element('style', {
				'id': 'table-name',
				'colors': '#000000',
			})
			tnode.append(node)

			node = lxml.etree.Element('style', {
				'id': 'table-schema-name',
				'colors': '#000000',
			})
			tnode.append(node)

			node = lxml.etree.Element('style', {
				'id': 'table-title',
				'colors': "{},{},{}".format(color, color, bcolor),
			})
			tnode.append(node)

			node = lxml.etree.Element('comment')
			node.text = layer['name']
			tnode.append(node)

		# Create custom domains for unsigned int types
		for it in ('smallint', 'integer', 'bigint'):
			dname = 'u' + it
			domains.add(dname)
			self._addDomainNodes(root, dname, it, 'gt0', 'VALUE >= 0')

		# Create tables
		for table in tables:
			figure = diagram.getTableFigure(table)
			layer = diagram.getFigureLayer(figure)

			tnode = lxml.etree.Element('table', {
				'name': table['name'],
				'layer': '0',
				'collapse-mode': "2",
				'max-obj-count': "0",
			})

			snode = lxml.etree.Element('schema', {
				'name': 'public',
			})
			tnode.append(snode)

			rnode = lxml.etree.Element('role', {
				'name': 'postgres',
			})
			tnode.append(rnode)

			node = lxml.etree.Element('tag', {
				'name': layer['name'].lower(),
			})
			tnode.append(node)

			pnode = lxml.etree.Element('position', {
				'x': str(figure['left'] + layer['left']),
				'y': str(figure['top'] + layer['top']),
			})
			tnode.append(pnode)

			#TODO:
			tabAI = table['nextAutoInc']
			tabPK = table['primaryKey']

			for col in table.columns:
				colnode = lxml.etree.Element('column', {
					'name': col['name'],
				})
				tnode.append(colnode)

				attrs = {
					'length': '0',
				}

				#TODO
				#print(col)
				#print()
				ai = True if col['autoIncrement'] else False
				dv = col['defaultValue']
				dvn = col['defaultValueIsNull']
				flags = col['flags']
				nn = True if col['isNotNull'] else False
				length = col['length']
				precision = col['precision']
				scale = col['scale']

				if col.type.type in ('SMALLINT', 'JSON', 'DECIMAL', 'VARCHAR', 'BIGINT', 'DATE', 'CHAR'):
					type = col.type.type.lower()
				elif col.type.type == 'INT':
					type = 'integer'
				elif col.type.type == 'TINYINT':
					if isinstance(col.type, UserType) and col.type['name'] in ('UBOOL', 'BOOLEAN', 'BOOL'):
						type = 'boolean'
					else:
						type = 'smallint'
				elif col.type.type == 'FLOAT':
					type = 'real'
				elif col.type.type == 'DOUBLE':
					type = 'double precision'
				elif col.type.type in ('TIMESTAMP', 'DATETIME', 'TIMESTAMP_F', 'DATETIME_F'):
					type = 'timestamp with time zone'
					attrs['with-timezone'] = 'true'
				elif col.type.type == 'TIME':
					type = 'time with time zone'
					attrs['with-timezone'] = 'true'
				elif col.type.type == 'TINYTEXT':
					type = 'varchar'
					attrs['length'] = '255'
				elif col.type.type == 'TEXT':
					type = 'varchar'
					attrs['length'] = '65535'
				elif col.type.type == 'MEDIUMTEXT':
					type = 'varchar'
					attrs['length'] = '16777215'
				elif col.type.type == 'LONGTEXT':
					type = 'varchar'
					attrs['length'] = '4294967295'
				elif col.type.type == 'ENUM':
					type = 'enum_' + str(enumid)
					enumid += 1

					# Add an enum type node
					utypenode = lxml.etree.Element('usertype', {
						'name': type,
						'configuration': 'enumeration',
					})
					root.append(utypenode)

					utypenode.append(lxml.etree.Element('schema', {
						'name': 'public',
					}))
					utypenode.append(lxml.etree.Element('role', {
						'name': 'postgres',
					}))
					utypenode.append(lxml.etree.Element('enumeration', {
						#TODO: enum list
						'values': 'abc,def',
					}))

					type = 'public.' + type
				else:
					self.log.warn('Unknown type %s', col.type.type)
					type = 'smallint'

				# Apply not-null
				if nn:
					colnode.set('not-null', "true")

				# Apply auto-increment
				if ai:
					if type in ('smallint', 'integer', 'bigint'):
						colnode.set('identity-type', "ALWAYS")

				# Apply default value
				#TODO: ON UPDATE CURRENT TIMESTAMP
				if dv:
					assert not dvn

					if dv in ('0', '1', 'TRUE', 'FALSE', 'CURRENT_TIMESTAMP'):
						pass
					elif dv.startswith("'") and dv.endswith("'"):
						pass
					elif dv == 'CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP':
						#TODO
						dv = 'CURRENT_TIMESTAMP'
					else:
						self.log.warn('Unknown default value %s', dv)
					colnode.set('default-value', dv)
				elif dvn:
					self.log.warn('Unsupported Null default value %s', dv)

				# Apply length/precision/scale
				if length > 0:
					assert precision < 0 and scale < 0, (length,precision,scale)
					assert attrs['length'] == '0'

					attrs['length'] = str(length)
				elif precision > 0:
					assert length < 0, (length,precision,scale)

					if scale < 0:
						# Since precision is not supported in pgSQL, create a domain for it

						dname = type + str(precision)
						if 'UNSIGNED' in flags:
							dname = 'u' + dname
						if dname not in domains:
							if 'UNSIGNED' in flags:
								minVal = 0
							else:
								minVal = '-' + '9' * precision
							maxVal = '9' * precision
							self._addDomainNodes(root, dname, type, 'range' + str(precision),
									'VALUE >= {} AND VALUE <= {}'.format(minVal, maxVal))
							domains.add(dname)
						type = 'public.' + dname
						flags.remove('UNSIGNED')
					else:
						assert attrs['length'] == '0'
						attrs['length'] = str(precision)
						attrs['precision'] = str(scale)

				elif scale > 0:
					assert False, (length,precision,scale)

				# Apply flags
				if flags:
					for flag in flags:
						if flag == 'UNSIGNED':
							if type in ('smallint', 'integer', 'bigint'):
								if ai:
									self.log.info('Unsupported domain in indentity column "%s.%s"',
											table['name'], col['name'])
								else:
									# Unsigned is not supported in PGSQL, so use a special domain
									type = 'public.u' + type
							else:
								self.log.error('Unsupported unsigned flag on %s field', type)
						else:
							self.log.warn('Unsupported flag: %s', flag)

				# TODO: integer/text precision
				# TODO: default
				# TODO: attribs
				# TODO: alias

				typenode = lxml.etree.Element('type', {
					'name': type,
				})
				for k, v in attrs.items():
					typenode.set(k, v)
				colnode.append(typenode)

				if 'comment' in col:
					commentnode = lxml.etree.Element('comment')
					commentnode.text = col['comment']
					colnode.append(commentnode)

			# Append at the end since enums must go above
			root.append(tnode)

			#TODO
			# Append PK
			#<constraint name="table_pk" type="pk-constr" table="public.new_table">
			#    <columns names="aaa" ref-type="src-columns"/>
			#</constraint>

		return tree

	def convert(self, mwbPath):
		''' Perform the conversion

		@param mwbPath string: The source file path
		'''

		# Extract XML from zip
		with zipfile.ZipFile(mwbPath, 'r') as mwbFile:
			if self.MWB_INNER_FILE not in mwbFile.namelist():
				raise InvalidFileFormatException(self.MWB_INNER_FILE + ' not found')

			with mwbFile.open(self.MWB_INNER_FILE) as xmlFile:
				tree = lxml.etree.parse(xmlFile)

		root = tree.getroot()

		# Basic XML validation
		if root.get('grt_format') != '2.0':
			raise InvalidFileFormatException(str(root.attrib))
		if root.get('document_type') != 'MySQL Workbench Model':
			raise InvalidFileFormatException(str(root.attrib))

		#for f in root.findall(".//value[@id='f1390fb2-1434-11e7-b731-10bf48baca66']"):
		#	print(tree.getelementpath(f))
		#raise RuntimeError()

		# Retrieve the document element
		document = root[0]
		assert document.tag == 'value', document.tag
		assert document.get('struct-name') == 'workbench.Document', document.attrib

		# Rietrieve the physical model element
		# TODO: support multiple models
		# Childs:
		#value {'type': 'object', 'struct-name': 'workbench.logical.Model', 'id': '4cd05de8-5bd6-11e1-bc3b-e0cb4ec5d89b', 'struct-checksum': '0xf4220370', 'key': 'logicalModel'}
		#value {'_ptr_': '0x5589fd96ef40', 'type': 'list', 'content-type': 'object', 'content-struct-name': 'workbench.OverviewPanel', 'key': 'overviewPanels'}
		#value {'_ptr_': '0x5589fd972b10', 'type': 'list', 'content-type': 'object', 'content-struct-name': 'workbench.physical.Model', 'key': 'physicalModels'}
		#value {'_ptr_': '0x558a018537d0', 'type': 'dict', 'key': 'customData'}
		#value {'type': 'object', 'struct-name': 'app.DocumentInfo', 'id': '4cd02f44-5bd6-11e1-bc3b-e0cb4ec5d89b', 'struct-checksum': '0xbba780b8', 'key': 'info'}
		#value {'type': 'object', 'struct-name': 'app.PageSettings', 'id': '4cd02bc0-5bd6-11e1-bc3b-e0cb4ec5d89b', 'struct-checksum': '0x7dc77977', 'key': 'pageSettings'}
		#value {'type': 'string', 'key': 'name'}
		models = document.findall("./value[@key='physicalModels']/value[@struct-name='workbench.physical.Model']")
		assert models, list(document)
		assert len(models) == 1, list(models)

		dbmTree = self.convertModel(models[0])
		with open('/tmp/test.dbm', 'wb') as out:
			out.write(lxml.etree.tostring(dbmTree, pretty_print=True))

	def convertModel(self, model):
		#value {'type': 'object', 'struct-name': 'db.mysql.Catalog', 'id': '4cd06db0-5bd6-11e1-bc3b-e0cb4ec5d89b', 'struct-checksum': '0x82ad3466', 'key': 'catalog'}
		#value {'type': 'string', 'key': 'connectionNotation'}
		#value {'_ptr_': '0x558a018542b0', 'type': 'list', 'content-type': 'object', 'content-struct-name': 'db.mgmt.Connection', 'key': 'connections'}
		#value {'_ptr_': '0x558a01854130', 'type': 'list', 'content-type': 'object', 'content-struct-name': 'workbench.physical.Diagram', 'key': 'diagrams'}
		#value {'type': 'string', 'key': 'figureNotation'}
		#value {'_ptr_': '0x558a01854320', 'type': 'list', 'content-type': 'object', 'content-struct-name': 'GrtStoredNote', 'key': 'notes'}
		#link {'type': 'object', 'struct-name': 'db.mgmt.Rdbms', 'key': 'rdbms'}
		#value {'_ptr_': '0x558a01854390', 'type': 'list', 'content-type': 'object', 'content-struct-name': 'db.Script', 'key': 'scripts'}
		#value {'_ptr_': '0x558a01854400', 'type': 'dict', 'key': 'syncProfiles'}
		#value {'_ptr_': '0x558a01854480', 'type': 'list', 'content-type': 'object', 'content-struct-name': 'GrtObject', 'key': 'tagCategories'}
		#value {'_ptr_': '0x558a018544f0', 'type': 'list', 'content-type': 'object', 'content-struct-name': 'meta.Tag', 'key': 'tags'}
		#link {'type': 'object', 'struct-name': 'model.Diagram', 'key': 'currentDiagram'}
		#value {'_ptr_': '0x558a018540b0', 'type': 'dict', 'key': 'customData'}
		#value {'_ptr_': '0x558a018541a0', 'type': 'list', 'content-type': 'object', 'content-struct-name': 'model.Marker', 'key': 'markers'}
		#value {'_ptr_': '0x558a01854210', 'type': 'dict', 'key': 'options'}
		#value {'type': 'string', 'key': 'name'}
		#link {'type': 'object', 'struct-name': 'GrtObject', 'key': 'owner'}

		catalog = model.find("./value[@key='catalog']")
		assert catalog is not None, list(model)

		schema = catalog.find("./value[@key='schemata']/value[@struct-name='db.mysql.Schema']")
		assert schema is not None, list(catalog)

		schemaNameTag = schema.find("./value[@key='name']")
		schemaName = schemaNameTag.text

		simpleTypesTag = catalog.find("./value[@key='simpleDatatypes']")
		assert simpleTypesTag is not None, list(catalog)

		userTypesTag = catalog.find("./value[@key='userDatatypes']")
		assert userTypesTag is not None, list(catalog)

		types = {}
		for st in simpleTypesTag:
			t = SimpleType(st)
			assert t.id not in types
			types[t.id] = t
		for ut in userTypesTag:
			t = UserType(ut)
			assert t.id not in types
			types[t.id] = t

		tables = schema.find("./value[@key='tables']")
		assert len(tables), list(schema)

		convTables = []
		for table in tables:
			convTables.append(Table(table, types))

		diagrams = model.find("./value[@key='diagrams']")
		assert len(diagrams), list(schema)

		convDiagrams = []
		for diagram in diagrams:
			assert diagram.get('struct-name') == 'workbench.physical.Diagram'
			convDiagrams.append(Diagram(diagram))

		#TODO: multiple diagrams not supported, choose diagram
		self.log.info('Using diagram "%s"', convDiagrams[0]['name'])
		return self.createDbm(schemaName, convTables, convDiagrams[0])


if __name__ == '__main__':
	import argparse

	parser = argparse.ArgumentParser(description='Convert a schema from MySQL Workbench to pgModeler format')
	parser.add_argument('mwb', help='the mwb source')

	args = parser.parse_args()

	logging.basicConfig(level=logging.DEBUG)

	Main().convert(args.mwb)
