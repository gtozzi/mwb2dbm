#!/usr/bin/env python3

import logging
import zipfile
import collections
import lxml.etree


class InvalidFileFormatException(RuntimeError):
	pass


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


class Column(BaseObjFromEl):
	def __init__(self, el):
		super().__init__(el)

		flags = el.find("./value[@key='flags']")

		self['flags'] = []
		for flag in flags:
			self['flags'].append(flag.text)


class Table(BaseObjFromEl):
	def __init__(self, el):
		super().__init__(el)

		columns = el.find("./value[@key='columns']")
		assert len(columns), list(el)

		self.columns = []
		for column in columns:
			self.columns.append(Column(column))


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

		raise NameError()

	def getFigureLayer(self, figure):
		for layer in self.layers:
			if layer.id == figure['layer']:
				return layer

		raise NameError()



class Main:
	''' Convert from MySQL Workbench to pgModeler '''

	MWB_INNER_FILE = 'document.mwb.xml'

	def __init__(self):
		self.log = logging.getLogger('main')

	def createDbm(self, dbname, tables, diagram):
		''' Creates a new empty DBM '''
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

		for table in tables:
			figure = diagram.getTableFigure(table)
			layer = diagram.getFigureLayer(figure)

			tnode = lxml.etree.Element('table', {
				'name': table['name'],
				'layer': '0',
				'collapse-mode': "2",
				'max-obj-count': "0",
			})
			root.append(tnode)

			snode = lxml.etree.Element('schema', {
				'name': 'public',
			})
			tnode.append(snode)

			rnode = lxml.etree.Element('role', {
				'name': 'postgres',
			})
			tnode.append(rnode)

			pnode = lxml.etree.Element('position', {
				'x': str(figure['left'] + layer['left']),
				'y': str(figure['top'] + layer['top']),
			})
			tnode.append(pnode)

			for col in table.columns:
				colnode = lxml.etree.Element('column', {
					'name': col['name'],
				})
				tnode.append(colnode)

				#TODO
				typenode = lxml.etree.Element('type', {
					'name': 'smallint',
					'length': '0',
				})
				colnode.append(typenode)

				if 'comment' in col:
					commentnode = lxml.etree.Element('comment')
					commentnode.text = col['comment']
					colnode.append(commentnode)

		for layer in diagram.layers:
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

		#for f in root.findall(".//value[.='Prodotti']"):
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

		tables = schema.find("./value[@key='tables']")
		assert len(tables), list(schema)

		convTables = []
		for table in tables:
			convTables.append(Table(table))

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
