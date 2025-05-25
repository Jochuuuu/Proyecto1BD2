import os
import struct
import json
import re
from pathlib import Path
from estructuras.hash import ExtendibleHashFile
from estructuras.avl import AVLFile
from estructuras.btree import BPlusTree

class TableStorageManager:
    """
    Gestiona una tabla de almacenamiento con registros adaptados según los atributos de la tabla
    usando una estrategia de lista libre para reutilizar registros eliminados.
    Versión actualizada con soporte completo para DELETE.
    """
    
    # Constantes para los valores especiales de 'next'
    RECORD_NORMAL = -2  # Registro normal (no eliminado)
    RECORD_END = -1     # Último registro eliminado en la lista libre
    
    # Mapeo de tipos de datos a formatos de struct
    TYPE_FORMATS = {
        'INT': 'i',         # 4 bytes para enteros
        'DECIMAL': 'd',     # 8 bytes para decimales (double)
        'CHAR': 's',        # Char de longitud fija
        'VARCHAR': 's',     # Varchar de longitud fija
        'BOOL': '?',        # 1 byte para booleanos
        'DATE': 'I'         # 4 bytes para fechas (timestamp)
    }
    
    # Tamaños por defecto para los tipos de datos (en bytes)
    TYPE_SIZES = {
        'INT': 4,
        'DECIMAL': 8,
        'BOOL': 1,
        'DATE': 4
    }
    
    def __init__(self, table_name, table_info, base_dir='tablas'):
        """
        Inicializa el administrador de la tabla.
        
        Args:
            table_name: Nombre de la tabla
            table_info: Diccionario con información sobre la estructura de la tabla
            base_dir: Directorio base donde se almacenarán los archivos de la tabla
        """
        self.table_name = table_name
        self.table_info = table_info
        self.base_dir = base_dir
        
        # Asegurar que el directorio base existe
        os.makedirs(base_dir, exist_ok=True)
        os.makedirs('indices', exist_ok=True)  
        
        # Nombre del archivo binario de la tabla
        self.filename = os.path.join(base_dir, f"{table_name}.bin")
        
        # Crear formatos de struct para los atributos
        self.attribute_formats = {}
        self.attribute_sizes = {}
        self.indices = {}
        self.primary_key_attr = None

        # Crear formato para cada atributo
        record_format_parts = []
        for attr in table_info['attributes']:
            fmt, size = self._get_format_for_attribute(attr)
            self.attribute_formats[attr['name']] = fmt
            self.attribute_sizes[attr['name']] = size
            record_format_parts.append(fmt)

            if attr.get('is_key', False):
                self.primary_key_attr = attr['name']

        # Añadir formato para el campo 'next' que se usa para la lista libre
        self.next_format = 'i'  # 4 bytes para next (int)
        record_format_parts.append(self.next_format)
        
        # Formato completo del registro
        self.record_format = f"<{''.join(record_format_parts)}"
        
        # Tamaño del registro en bytes
        self.record_size = struct.calcsize(self.record_format)
 
        # Tamaño del encabezado (contiene el puntero a la cabecera de la lista libre)
        self.header_format = "<i"  # 4 bytes para la cabecera (int)
        self.header_size = struct.calcsize(self.header_format)
        
        INDEX_CLASSES = {
            'hash': ExtendibleHashFile,
            'avl': AVLFile,
            'btree': BPlusTree,  
            'isam' : BPlusTree
        }

        for attr_index, attr in enumerate(table_info['attributes'], 1):
            if attr.get('index'):
                index_type = attr['index'].lower()
                if index_type not in INDEX_CLASSES:
                    print(f"Advertencia: Tipo de índice '{attr['index']}' no soportado para {attr['name']}, usando AVL")
                    index_type = 'avl'
                
                # Parámetros comunes
                index_params = {
                    'record_format': self.record_format,
                    'index_attr': attr_index,
                    'table_name': self.table_name,
                    'is_key': attr.get('is_key', False)
                }
                
                # Crear el índice
                index_class = INDEX_CLASSES[index_type]
                self.indices[attr['name']] = index_class(**index_params)

        # Crear el archivo si no existe
        if not os.path.exists(self.filename):
            self._initialize_file()
    
    def _get_format_for_attribute(self, attr):
        """
        Obtiene el formato de struct y tamaño para un atributo.
        
        Args:
            attr: Diccionario con información del atributo
        
        Returns:
            Tupla (formato, tamaño)
        """
        data_type = attr['data_type'].upper()
        
        size_match = re.match(r'(VARCHAR|CHAR)\[(\d+)\]', data_type)
        
        if size_match:
            base_type = size_match.group(1)
            size = int(size_match.group(2))
            return f"{size}s", size
        
        # Para los tipos básicos
        for base_type in self.TYPE_FORMATS:
            if data_type.startswith(base_type):
                return self.TYPE_FORMATS[base_type], self.TYPE_SIZES.get(base_type, 4)
        
        # Tipo no reconocido, usar un formato genérico
        return "i", 4
    
    def _initialize_file(self):
        """Inicializa el archivo con un encabezado que indica que no hay registros eliminados."""
        with open(self.filename, 'wb') as f:
            f.write(struct.pack(self.header_format, -1))
            
           
    
    def _read_header(self):
        """Lee el valor de la cabecera (puntero al primer registro eliminado)."""
        with open(self.filename, 'rb') as f:
            header_data = f.read(self.header_size)
            return struct.unpack(self.header_format, header_data)[0]
    
    def _write_header(self, header_value):
        """Actualiza el valor de la cabecera."""
        with open(self.filename, 'r+b') as f:
            f.write(struct.pack(self.header_format, header_value))
    
    def _get_record_position(self, id):
        """
        Calcula la posición del registro en el archivo basado en su id.
        Asume que los ids son consecutivos empezando desde 1.
        """
        return self.header_size + (id - 1) * self.record_size
    
    def _read_record(self, id):
        """Lee un registro específico por su id."""
        position = self._get_record_position(id)
        
        with open(self.filename, 'rb') as f:
            f.seek(position)
            record_data = f.read(self.record_size)
            
            if not record_data or len(record_data) < self.record_size:
                return None
            
            # Desempaquetar los datos en una lista
            unpacked_data = list(struct.unpack(self.record_format, record_data))
            
            # Convertir los valores de tipo string (eliminando padding)
            result = {}
            data_index = 0
            
            for attr in self.table_info['attributes']:
                value = unpacked_data[data_index]
                
                # Convertir strings y eliminar padding
                if attr['data_type'].upper().startswith(('VARCHAR', 'CHAR')):
                    value = value.decode('utf-8').rstrip('\x00')
                
                result[attr['name']] = value
                data_index += 1
            
            # El último valor siempre es el puntero 'next'
            result['next'] = unpacked_data[-1]
            
            return result
    
    def _pack_record_data(self, record_data):
        """
        Empaqueta los datos de un registro para almacenarlos.
        
        Args:
            record_data: Diccionario con los datos del registro
        
        Returns:
            Datos empaquetados listos para escribir en el archivo
        """
        values = []
        
        # Empaquetar cada atributo en el orden definido en table_info
        for attr in self.table_info['attributes']:
            attr_name = attr['name']
            value = record_data.get(attr_name)
            
            # Convertir según el tipo de dato
            if attr['data_type'].upper().startswith(('VARCHAR', 'CHAR')):
                size_match = re.match(r'(VARCHAR|CHAR)\[(\d+)\]', attr['data_type'].upper())
                if size_match:
                    size = int(size_match.group(2))
                    # Asegurar que es una cadena y codificarla
                    if not isinstance(value, str):
                        value = str(value)
                    value = value.encode('utf-8')[:size].ljust(size, b'\x00')
            
            values.append(value)
        
        # Añadir el valor de 'next'
        values.append(record_data.get('next', self.RECORD_NORMAL))
        
        # Empaquetar todos los valores
        return struct.pack(self.record_format, *values)
    
    def _write_record(self, id, record_data):
        """
        Escribe un registro en la posición correspondiente a su id.
        
        Args:
            id: ID para posicionar el registro
            record_data: Diccionario con los datos del registro
        """
        position = self._get_record_position(id)
        
        # Empaquetar los datos
        packed_data = self._pack_record_data(record_data)
        
        # Escribir en el archivo
        with open(self.filename, 'r+b') as f:
            f.seek(position)
            f.write(packed_data)
    
    def _get_file_size(self):
        """Obtiene el tamaño actual del archivo."""
        return os.path.getsize(self.filename)
    
    def _get_record_count(self):
        """Obtiene el número de registros en el archivo (incluyendo los eliminados)."""
        file_size = self._get_file_size()
        return (file_size - self.header_size) // self.record_size
    
    def _update_indices(self, record, id):
        """
        Actualiza los índices para un registro específico.
        
        Args:
            record: Diccionario con los datos del registro
            id: ID del registro
        """
        for attr_name, index in self.indices.items():
            if attr_name in record:
                value = record[attr_name]
                if value not in index:
                    index[value] = []
                
                # Asegurarse de que el ID no está duplicado en el índice
                if id not in index[value]:
                    index[value].append(id)
    
    def _remove_from_indices(self, record, id):
        """
        Elimina un registro de los índices (método legacy).
        
        Args:
            record: Diccionario con los datos del registro
            id: ID del registro
        """
        for attr_name, index in self.indices.items():
            if attr_name in record:
                value = record[attr_name]
                if value in index and id in index[value]:
                    index[value].remove(id)
                    
                    # Si la lista está vacía, eliminar la entrada del índice
                    if not index[value]:
                        del index[value]
    
    def insert(self, record_data):
        """
        Inserta un nuevo registro, reutilizando espacios eliminados si están disponibles.
        
        Args:
            record_data: Diccionario con los datos del registro
        
        Returns:
            ID del registro insertado
        """
        for attr in self.table_info['attributes']:
            if attr['name'] not in record_data:
                raise ValueError(f"Falta el atributo {attr['name']} en los datos del registro")
            
        if self.primary_key_attr in self.indices:
            primary_key_value = record_data[self.primary_key_attr]
            index = self.indices[self.primary_key_attr]
            
            existing_records = index.search(primary_key_value)
            if existing_records:
                return None
        
        cabecera = self._read_header()
        record_id = None
        
        if cabecera != -1:
            deleted_record = self._read_record(cabecera)
            next_free = deleted_record['next']
            
            self._write_header(next_free)
            
            record_id = cabecera
            
            record_data['next'] = self.RECORD_NORMAL
            
            self._write_record(record_id, record_data)
        else:
            record_count = self._get_record_count()
            record_id = record_count + 1
            
            record_data['next'] = self.RECORD_NORMAL
            
            self._write_record(record_id, record_data)
        
        for attr_name, index in self.indices.items():
            index.insert_record(record_id)

        return record_id

    def delete(self, id):
        """
        Marca un registro como eliminado y lo agrega a la lista de registros libres.
        VERSIÓN CORREGIDA que NO elimina de índices (eso se hace en delete_records).
        
        Args:
            id: ID del registro a eliminar
            
        Returns:
            True si se eliminó correctamente, False si no se encontró
        """
        record = self._read_record(id)
        if not record:
            return False
        
        if record['next'] != self.RECORD_NORMAL:
            return False
        
       
        
        cabecera = self._read_header()
        
        record['next'] = cabecera
        self._write_record(id, record)
        
        self._write_header(id)
        
        return True


    def delete_records(self, record_numbers):
        """
        Elimina múltiples registros especificados por sus números de registro.
        VERSIÓN CON DEBUG COMPLETO para mostrar estado de índices.
        
        Args:
            record_numbers (list): Lista de números de registro a eliminar
            
        Returns:
            int: Cantidad de registros eliminados exitosamente
        """
        if not record_numbers:
            print("No hay registros para eliminar")
            return 0
        
        
        deleted_count = 0
        failed_records = []
        
        for record_num in record_numbers:
            try:
                
                record = self._read_record(record_num)
                if not record or record.get('next') != self.RECORD_NORMAL:
                    #print(f"Registro {record_num} no existe o ya fue eliminado")
                    failed_records.append(record_num)
                    continue
                
                #print(f"Registro a eliminar: {dict((k, v) for k, v in record.items() if k != 'next')}")
                '''

                print(f"\n🔍 ESTADO DE ÍNDICES ANTES DE ELIMINAR REGISTRO {record_num}:")
                for attr_name, index in self.indices.items():
                    attr_value = record.get(attr_name)
                    print(f" indice {attr_name} (valor: '{attr_value}'):")
                    
                    if hasattr(index, 'search'):
                        results = index.search(attr_value)
                        print(f" Busqueda de '{attr_value}': {results}")
                        if record_num in results:
                            print(f" Registro {record_num} SÍ está en el índice")
                        else:
                            print(f" Registro {record_num} NO está en el índice")
                '''

                indices_success = True
                
                for attr_name, index in self.indices.items():
                    try:
                        if hasattr(index, 'delete_record'):
                            if attr_name == 'nombre': 
                                result = index.delete_record(record_num)
                            else:
                                result = index.delete_record(record_num)
                            '''
                            if result is not None:
                                #print(f" Eliminado del índice {attr_name}")
                            else:
                                #print(f" No se encontró en el índice {attr_name}") '''
                        else:
                            '''
                            if isinstance(index, dict) and attr_name in record:
                                value = record[attr_name]
                                if value in index and record_num in index[value]:
                                    index[value].remove(record_num)
                                    if not index[value]:
                                        del index[value]
                                    print(f"Eliminado del indice {attr_name} (legacy)")
                                else:
                                    print(f"No se encontró en indice {attr_name} (legacy)")
                            else:
                                print(f" Tipo de indice no soportado: {type(index).__name__}") '''
                            print("no deberia llegar aqui")
                                
                    except Exception as e:
                        print(f"  Error al eliminar del índice {attr_name}: {e}")
                        indices_success = False
                '''
                print(f"\nESTADO DE ÍNDICES DESPUÉS DE ELIMINAR REGISTRO {record_num}:")
                for attr_name, index in self.indices.items():
                    attr_value = record.get(attr_name)
                    print(f"Índice {attr_name} (valor: '{attr_value}'):")
                    if hasattr(index, 'search'):
                        results = index.search(attr_value)
                        print(f" Búsqueda de '{attr_value}': {results}")
                        if record_num in results:
                            print(f" ERROR: Registro {record_num} AÚN está en el índice")
                        else:
                            print(f"Registro {record_num} correctamente eliminado del índice")
                    
                    if hasattr(index, 'print_tree'):
                        index.print_tree()
                '''
                if indices_success:
                    if self.delete(record_num):
                        #print(f"Registro {record_num} eliminado exitosamente")
                        deleted_count += 1
                    else:
                        #print(f" Error al marcar registro {record_num} como eliminado")
                        failed_records.append(record_num)
                else:
                    #print(f" No se pudo eliminar de todos los indices, registro {record_num} no eliminado")
                    failed_records.append(record_num)
                    
            except Exception as e:
                print(f"Error general al eliminar registro {record_num}: {e}")
                failed_records.append(record_num)
        
        '''
        for attr_name, index in self.indices.items():
            
            if hasattr(index, 'print_tree'):
                index.print_tree()
            elif hasattr(index, 'print_file_structure'):
                print(f" Estructura del archivo:")
                index.print_file_structure()
            elif isinstance(index, dict):
                print(f"   Contenido del diccionario:")
                if index:
                    for value, record_ids in index.items():
                        print(f" '{value}': {record_ids}")
                else:
                    print(f"(vacío)") '''
        '''
        if failed_records:
            print(f"\nRegistros que no pudieron eliminarse: {failed_records}") '''
        
        #print(f"\n Total eliminados: {deleted_count}/{len(record_numbers)}")
        return deleted_count

    def _remove_from_all_indices(self, record, record_num):
        """
        Elimina un registro de todos los índices usando delete_record().
        
        Args:
            record (dict): Datos del registro
            record_num (int): Número del registro
        """
        #print(f"  Eliminando registro {record_num} de los índices...")
        
        for attr_name, index in self.indices.items():
            try:
                if hasattr(index, 'delete_record'):
                    result = index.delete_record(record_num)
                    
                else:
                   
                    if attr_name in record:
                        value = record[attr_name]
                        if value in index and record_num in index[value]:
                            index[value].remove(record_num)
                            if not index[value]:
                                del index[value]
                            
                            
            except Exception as e:
                print(f" Error al eliminar del índice {attr_name}: {e}")

    def select(self, lista_busquedas=None, lista_rangos=None, requested_attributes=None):
        """
        Busca registros que cumplan todas las condiciones especificadas.
        Si no hay condiciones, retorna TODOS los registros.
        
        Args:
            lista_busquedas: Lista de búsquedas exactas en formato [["atributo", valor], ...]
            lista_rangos: Lista de búsquedas por rango en formato [["atributo", min_val, max_val], ...]
            requested_attributes: Lista de atributos a retornar. Si es None, retorna todos.
            
        Returns:
            dict: Resultado con números de registro o información de errores
        """
        if not lista_busquedas and not lista_rangos:
            print("No hay condiciones WHERE - retornando todos los registros")
            all_records = self._get_all_active_record_numbers()
            return {
                "error": False, 
                "numeros_registro": all_records,
                "requested_attributes": requested_attributes
            }
        
        conjuntos_resultados = []
        errores = []
        
        # Procesar búsquedas exactas
        if lista_busquedas:
            for i, (attr_name, valor) in enumerate(lista_busquedas):
                #print(f"Búsqueda exacta {i+1}: {attr_name} = {valor}")
                
                if attr_name in self.indices:
                    indice = self.indices[attr_name]
                    resultados = indice.search(valor)
                    
                    #print(f"  Índice {attr_name} encontró: {resultados}")
                    
                    if not resultados:
                        #print(f"  No hay resultados para {attr_name}={valor}")
                        return {"error": False, "numeros_registro": [], "requested_attributes": requested_attributes}
                    
                    conjuntos_resultados.append(set(resultados))
                    
                else:
                    error_msg = f"No existe índice para {attr_name}"
                    #print(f"  ERROR: {error_msg}")
                    errores.append({"error": True, "message": error_msg, "type": "no_index"})
        
        # Procesar búsquedas por rango
        if lista_rangos:
            for i, (attr_name, min_val, max_val) in enumerate(lista_rangos):
                #print(f"Búsqueda por rango {i+1}: {attr_name} BETWEEN {min_val} AND {max_val}")
                
                if attr_name in self.indices:
                    indice = self.indices[attr_name]
                    resultado = indice.range_search(min_val, max_val)
                    
                    # Verificar si es un error (índice no soporta rangos)
                    if isinstance(resultado, dict) and resultado.get("error", False):
                        #print(f"  ERROR: {resultado['message']}")
                        errores.append(resultado)
                        continue
                    
                    # Si no es error, debe ser lista de números de registro
                    #print(f"  Índice {attr_name} encontró en rango: {resultado}")
                    
                    if not resultado:
                        #print(f"  No hay resultados en rango [{min_val}, {max_val}]")
                        return {"error": False, "numeros_registro": [], "requested_attributes": requested_attributes}
                    
                    conjuntos_resultados.append(set(resultado))
                    
                else:
                    error_msg = f"No existe índice para {attr_name}"
                   # print(f"  ERROR: {error_msg}")
                    errores.append({"error": True, "message": error_msg, "type": "no_index"})
        
        if errores:
            return {
                "error": True,
                "errores": errores,
                "message": "Se encontraron errores en la consulta"
            }
        
        if not conjuntos_resultados:
            return {
                "error": True,
                "message": "No se pudieron procesar las búsquedas",
                "errores": []
            }
        
        interseccion_final = conjuntos_resultados[0]
        
        for i, conjunto in enumerate(conjuntos_resultados[1:], 1):
            interseccion_final = interseccion_final.intersection(conjunto)
            
            if not interseccion_final:
                print("  Intersección vacía, terminando")
                return {"error": False, "numeros_registro": [], "requested_attributes": requested_attributes}
        
        resultado_final = (list(interseccion_final))
        
        return {
            "error": False, 
            "numeros_registro": resultado_final,
            "requested_attributes": requested_attributes
        }

    def _get_all_active_record_numbers(self):
        """
        Obtiene todos los números de registro activos (no eliminados) en la tabla.
        
        Returns:
            list: Lista de números de registro activos
        """
        record_count = self._get_record_count()
        active_records = []
        
        for i in range(1, record_count + 1):
            record = self._read_record(i)
            if record and record.get('next') == self.RECORD_NORMAL:
                active_records.append(i)
        
        print(f"Encontrados {len(active_records)} registros activos: {active_records}")
        return active_records

   

    def parse_sql_select(self, sql_statement):
        """
        Analiza una instrucción SQL SELECT y extrae las condiciones de búsqueda.
        VERSIÓN CORREGIDA que maneja SELECT sin WHERE correctamente.
        """
        select_pattern = re.compile(
            r"SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s*;)?$", 
            re.IGNORECASE | re.DOTALL
        )
        
        match = select_pattern.match(sql_statement.strip())
        if not match:
            return {
                "error": True,
                "message": "Formato de SELECT no válido"
            }
        
        columns_str = match.group(1).strip()
        table_name = match.group(2)
        where_clause = match.group(3)  #  None si no hay WHERE
        
        if table_name not in self.tables:
            return {
                "error": True,
                "message": f"La tabla '{table_name}' no existe"
            }
        
        requested_attributes = []
        if columns_str == "*":
            requested_attributes = [attr['name'] for attr in self.tables[table_name]['attributes']]
        else:
            attr_names = [attr.strip() for attr in columns_str.split(',')]
            
            table_attributes = {attr['name'] for attr in self.tables[table_name]['attributes']}
            
            for attr_name in attr_names:
                if attr_name not in table_attributes:
                    return {
                        "error": True,
                        "message": f"El atributo '{attr_name}' no existe en la tabla '{table_name}'"
                    }
            
            requested_attributes = attr_names
        
        lista_busquedas = []
        lista_rangos = []
        
        if where_clause:  
            try:
                lista_busquedas, lista_rangos = self._parse_where_with_ranges(where_clause, table_name)
            except Exception as e:
                return {
                    "error": True,
                    "message": f"Error al parsear condiciones WHERE: {str(e)}"
                }
        else:
            print("SELECT sin WHERE - se retornarán todos los registros")
        
        return {
            'error': False,
            'table_name': table_name,
            'columns': columns_str,
            'requested_attributes': requested_attributes,
            'lista_busquedas': lista_busquedas,
            'lista_rangos': lista_rangos
        }


    
    def has_select_error(self, resultado_select):
        """
        Verifica si el resultado de select() contiene errores.
        
        Args:
            resultado_select: Resultado del método select()
            
        Returns:
            bool: True si hay errores, False si es exitoso
        """
        return resultado_select.get("error", False)

    def get_select_errors(self, resultado_select):
        """
        Obtiene la lista de errores del resultado de select().
        
        Args:
            resultado_select: Resultado del método select()
            
        Returns:
            list: Lista de errores o lista vacía si no hay errores
        """
        if not self.has_select_error(resultado_select):
            return []
        
        return resultado_select.get("errores", [])

    def get_select_records(self, resultado_select):
        """
        Obtiene los números de registro del resultado de select().
        
        Args:
            resultado_select: Resultado del método select()
            
        Returns:
            list: Lista de números de registro o lista vacía si hay errores
        """
        if self.has_select_error(resultado_select):
            return []
        
        return resultado_select.get("numeros_registro", [])
    
    def update(self, id, record_data):
        """
        Actualiza un registro existente.
        
        Args:
            id: ID del registro a actualizar
            record_data: Diccionario con los datos actualizados
            
        Returns:
            True si se actualizó correctamente, False si no se encontró
        """
        current_record = self._read_record(id)
        if not current_record or current_record['next'] != self.RECORD_NORMAL:
            return False
        
        # Eliminar de los índices antes de actualizar
        self._remove_from_indices(current_record, id)
        
        # Actualizar los datos manteniendo el campo 'next'
        updated_record = {**current_record, **record_data, 'next': current_record['next']}
        
        # Escribir el registro actualizado
        self._write_record(id, updated_record)
        
        # Actualizar los índices con los nuevos datos
        self._update_indices(updated_record, id)
        
        return True
    
    def get(self, id):
        """
        Obtiene un registro por su ID.
        
        Args:
            id: ID del registro a buscar
            
        Returns:
            Diccionario con los datos del registro o None si no se encontró
        """
        record = self._read_record(id)
        
        if not record or record['next'] != self.RECORD_NORMAL:
            return None
        
        # Eliminar el campo 'next' para devolver solo los atributos de la tabla
        del record['next']
        return record
    
    def find_by_attribute(self, attr_name, value):
        """
        Busca registros por un atributo específico.
        
        Args:
            attr_name: Nombre del atributo por el que buscar
            value: Valor a buscar
            
        Returns:
            Lista de registros que coinciden con la búsqueda
        """
        # Si existe un índice para este atributo, usarlo
        if attr_name in self.indices and value in self.indices[attr_name]:
            result = []
            for record_id in self.indices[attr_name][value]:
                record = self.get(record_id)
                if record:
                    result.append(record)
            return result
        
        # Si no hay índice, hacer búsqueda secuencial
        result = []
        record_count = self._get_record_count()
        
        for i in range(1, record_count + 1):
            record = self._read_record(i)
            if (record and 
                record['next'] == self.RECORD_NORMAL and 
                attr_name in record and 
                record[attr_name] == value):
                
                # Eliminar el campo 'next' para devolver solo los atributos de la tabla
                record_copy = record.copy()
                del record_copy['next']
                result.append(record_copy)
        
        return result
    
    def get_all_records(self):
        """
        Obtiene todos los registros no eliminados.
        
        Returns:
            Lista de diccionarios con los datos de los registros
        """
        record_count = self._get_record_count()
        result = []
        
        for i in range(1, record_count + 1):
            record = self._read_record(i)
            if record and record['next'] == self.RECORD_NORMAL:
                # Eliminar el campo 'next' para devolver solo los atributos de la tabla
                record_copy = record.copy()
                del record_copy['next']
                result.append(record_copy)
        
        return result
    
    def rebuild_indices(self):
        """Reconstruye todos los índices leyendo los registros de la tabla."""
        for attr_name in self.indices:
            self.indices[attr_name] = {}
        
        record_count = self._get_record_count()
        
        for i in range(1, record_count + 1):
            record = self._read_record(i)
            if record and record['next'] == self.RECORD_NORMAL:
                self._update_indices(record, i)

