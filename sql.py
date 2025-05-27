import re
from pathlib import Path
import os
import csv
import json 
from estructuras.point_class import Point  # Importar la clase Point

class SQLTableManager:
    """
    Clase para gestionar tablas SQL, permitiendo analizar y almacenar
    definiciones de tablas a partir de declaraciones CREATE TABLE, INSERT INTO, SELECT, DELETE FROM e IMPORT FROM CSV.
    VERSIÓN ACTUALIZADA con soporte completo para tipo POINT.
    """
    
    def __init__(self, storage_class=None, base_dir='tablas'):
        """
        Inicializa el gestor de tablas.
        
        Args:
            storage_class: Clase que se usará para almacenamiento (e.g., TableStorageManager)
            base_dir: Directorio base para almacenar las tablas
        """
        self.tables = {}
        self.storage_managers = {}
        self.storage_class = storage_class
        self.base_dir = base_dir
        self.operations = []
        
        Path(base_dir).mkdir(exist_ok=True)
        
        self.load_existing_tables()

    def load_existing_tables(self):
        """
        Carga todas las tablas existentes desde el sistema de archivos.
        Busca archivos *_meta.json en el directorio base y recrea las estructuras.
        """
        if not os.path.exists(self.base_dir):
            return
        
        tables_found = 0
        
        # Buscar archivos de metadata
        for filename in os.listdir(self.base_dir):
            if filename.endswith('_meta.json'):
                table_name = filename.replace('_meta.json', '')
                metadata_path = os.path.join(self.base_dir, filename)
                
                try:
                    # Cargar metadata de la tabla
                    with open(metadata_path, 'r', encoding='utf-8') as f:
                        table_info = json.load(f)
                    
                    # Verificar que existe el archivo de datos
                    data_file = os.path.join(self.base_dir, f"{table_name}.bin")
                    if not os.path.exists(data_file):
                        continue
                    
                    self.tables[table_name] = table_info
                    
                    if self.storage_class:
                        self.storage_managers[table_name] = self.storage_class(
                            table_name, table_info, self.base_dir
                        )
                    
                    tables_found += 1
                    
                except Exception as e:
                    continue
    
    def parse_sql_statement(self, sql_statement):
        """
        Analiza una instrucción SQL que puede contener múltiples declaraciones.
        Versión actualizada que incluye CREATE, INSERT, SELECT, DELETE e IMPORT FROM CSV.
        
        Args:
            sql_statement (str): La instrucción SQL completa.
            
        Returns:
            list: Lista de operaciones procesadas.
        """
        # Limpiar el statement (eliminar comentarios, etc.)
        clean_sql = self._clean_sql_statement(sql_statement)
        
        # Extraer todas las operaciones SQL (CREATE TABLE, INSERT INTO, SELECT, DELETE, IMPORT FROM CSV, etc.)
        operations = self._extract_sql_operations(clean_sql)
        
        # Procesar cada operación en el orden que aparece
        processed_operations = []
        for op_type, op_content in operations:
            result = None
            
            if op_type == "CREATE":
                result = self._process_create_table(op_content)
                if result:
                    processed_operations.append(("CREATE", result))
            
            elif op_type == "INSERT":
                result = self._process_insert(op_content)
                if result:
                    processed_operations.append(("INSERT", result))
                    
            elif op_type == "SELECT":
                result = self._process_select(op_content)
                if result:
                    processed_operations.append(("SELECT", result))
                    
            elif op_type == "DELETE":
                result = self._process_delete(op_content)
                if result:
                    processed_operations.append(("DELETE", result))
            elif op_type == "IMPORT_CSV":
                result = self._process_import_csv(op_content)
                if result:
                    processed_operations.append(("IMPORT_CSV", result))
        
        self.operations.extend(processed_operations)
        return processed_operations
    
    def _clean_sql_statement(self, sql_statement):
        """
        Limpia una instrucción SQL eliminando comentarios y normalizando espacios en blanco.
        
        Args:
            sql_statement (str): La instrucción SQL original.
            
        Returns:
            str: La instrucción SQL limpia.
        """
        # Eliminar comentarios de una línea (--...)
        sql_no_comments = re.sub(r'--.*?$', '', sql_statement, flags=re.MULTILINE)
        
        # Eliminar comentarios multilínea (/* ... */)
        sql_no_comments = re.sub(r'/\*.*?\*/', '', sql_no_comments, flags=re.DOTALL)
        
        # Normalizar espacios en blanco
        return ' '.join(sql_no_comments.split())
    
    def _extract_sql_operations(self, sql_statement):
        """
        Extrae todas las operaciones SQL de un statement.
        Versión actualizada que incluye CREATE, INSERT, SELECT, DELETE e IMPORT FROM CSV.
        
        Args:
            sql_statement (str): La instrucción SQL limpia.
            
        Returns:
            list: Lista de tuplas (tipo_operación, contenido_operación).
        """
        # Dividir por punto y coma, pero teniendo cuidado con los strings que pueden contener punto y coma
        statements = []
        current = ""
        in_string = False
        string_char = None
        
        for char in sql_statement:
            if char in ["'", '"'] and (not in_string or string_char == char):
                in_string = not in_string
                if in_string:
                    string_char = char
                else:
                    string_char = None
            
            if char == ";" and not in_string:
                if current.strip():
                    statements.append(current.strip())
                current = ""
            else:
                current += char
        
        if current.strip():
            statements.append(current.strip())
        
        # Clasificar cada statement
        operations = []
        for stmt in statements:
            if re.match(r'^\s*CREATE\s+TABLE', stmt, re.IGNORECASE):
                operations.append(("CREATE", stmt))
            elif re.match(r'^\s*INSERT\s+INTO', stmt, re.IGNORECASE):
                operations.append(("INSERT", stmt))
            elif re.match(r'^\s*SELECT', stmt, re.IGNORECASE):
                operations.append(("SELECT", stmt))
            elif re.match(r'^\s*DELETE\s+FROM', stmt, re.IGNORECASE):
                operations.append(("DELETE", stmt))
            # ✨ NUEVO: Detectar IMPORT FROM CSV
            elif re.match(r'^\s*IMPORT\s+FROM\s+CSV', stmt, re.IGNORECASE):
                operations.append(("IMPORT_CSV", stmt))
            
        
        return operations
    
    def _process_create_table(self, sql_statement):
        """
        Procesa una instrucción CREATE TABLE.
        
        Args:
            sql_statement (str): La instrucción SQL CREATE TABLE.
            
        Returns:
            str: Nombre de la tabla creada, o None si hubo un error.
        """
        table_info = self.parse_sql_create_table(sql_statement)
        if table_info:
            table_name = table_info['table_name']
            self.tables[table_name] = table_info
            
            # Crear un gestor de almacenamiento para esta tabla si se proporcionó la clase
            if self.storage_class:
                # Si ya existe un gestor para esta tabla, lo eliminamos primero
                if table_name in self.storage_managers:
                    return table_name

                # Crear un nuevo gestor
                self.storage_managers[table_name] = self.storage_class(
                    table_name, table_info, self.base_dir
                )
            
            return table_name
        return None
    
    def _process_insert(self, sql_statement):
        """
        Procesa una instrucción INSERT INTO.
        
        Args:
            sql_statement (str): La instrucción SQL INSERT INTO.
            
        Returns:
            dict: Información del insert procesado, o None si hubo un error.
        """
        insert_info = self.parse_sql_insert(sql_statement)
        if insert_info:
            table_name = insert_info['table_name']
            records = insert_info['records']
            
            # Verificar que la tabla existe
            if table_name not in self.tables:
                return None
            
            # Si hay un gestor de almacenamiento para esta tabla, insertar los registros
            if table_name in self.storage_managers:
                storage_manager = self.storage_managers[table_name]
                inserted_ids = []
                
                for record in records:
                    try:
                        record_id = storage_manager.insert(record)
                        inserted_ids.append(record_id)
                    except Exception as e:
                        print(f"Error al insertar registro en tabla '{table_name}': {e}")
                
                return {
                    'table_name': table_name,
                    'records': records,
                    'inserted_ids': inserted_ids
                }
            else:
                print(f"Advertencia: No hay un gestor de almacenamiento para la tabla '{table_name}'.")
            
            return {
                'table_name': table_name,
                'records': records
            }
        
        return None
    
    def _process_select(self, sql_statement):
        """
        Procesa una instrucción SELECT y ejecuta la búsqueda.
        Versión actualizada que soporta rangos y atributos específicos.
        
        Args:
            sql_statement (str): La instrucción SQL SELECT.
            
        Returns:
            dict: Información del select procesado, incluyendo resultados.
        """
        select_info = self.parse_sql_select(sql_statement)
        
        # Si hubo error en el parsing, retornarlo directamente
        if select_info.get('error', False):
            print(f"Error en SELECT: {select_info['message']}")
            return select_info
        
        table_name = select_info['table_name']
        lista_busquedas = select_info['lista_busquedas']
        lista_rangos = select_info['lista_rangos']
        requested_attributes = select_info['requested_attributes']
        
        # Ejecutar la consulta si hay un gestor de almacenamiento
        if table_name in self.storage_managers:
            storage_manager = self.storage_managers[table_name]
            
            try:
                # Ejecutar el select con listas de búsquedas y rangos
                resultado = storage_manager.select(
                    lista_busquedas=lista_busquedas if lista_busquedas else None,
                    lista_rangos=lista_rangos if lista_rangos else None,
                    requested_attributes=requested_attributes
                )
                
                #storage_manager.print_select_results(resultado, f"SELECT en {table_name}")
                
                return {
                    'error': False,
                    'table_name': table_name,
                    'lista_busquedas': lista_busquedas,
                    'lista_rangos': lista_rangos,
                    'requested_attributes': requested_attributes,
                    'resultado': resultado
                }
                
            except Exception as e:
                error_result = {
                    'error': True,
                    'message': f"Error al ejecutar SELECT en tabla '{table_name}': {str(e)}"
                }
                print(f"Error: {error_result['message']}")
                return error_result
        else:
            error_result = {
                'error': True,
                'message': f"No hay un gestor de almacenamiento para la tabla '{table_name}'"
            }
            print(f"Error: {error_result['message']}")
            return error_result
    
    def _process_delete(self, sql_statement):
        """
        Procesa una instrucción DELETE FROM y ejecuta la eliminación.
        
        Args:
            sql_statement (str): La instrucción SQL DELETE FROM.
            
        Returns:
            dict: Información del delete procesado, incluyendo resultados.
        """
        delete_info = self.parse_sql_delete(sql_statement)
        
        # Si hubo error en el parsing, retornarlo directamente
        if delete_info.get('error', False):
            print(f"Error en DELETE: {delete_info['message']}")
            return delete_info
        
        table_name = delete_info['table_name']
        lista_busquedas = delete_info['lista_busquedas']
        lista_rangos = delete_info['lista_rangos']
        
        print(f"Procesando DELETE en tabla: {table_name}")
        if lista_busquedas:
            print(f"Condiciones exactas: {lista_busquedas}")
        if lista_rangos:
            print(f"Condiciones de rango: {lista_rangos}")
        
        # Verificar que la tabla existe
        if table_name not in self.tables:
            error_result = {
                'error': True,
                'message': f"La tabla '{table_name}' no existe"
            }
            print(f"Error: {error_result['message']}")
            return error_result
        
        # Ejecutar la eliminación si hay un gestor de almacenamiento
        if table_name in self.storage_managers:
            storage_manager = self.storage_managers[table_name]
            
            try:
                # Primero, encontrar los registros que cumplen las condiciones
                print("Buscando registros que cumplen las condiciones...")
                resultado_busqueda = storage_manager.select(
                    lista_busquedas=lista_busquedas if lista_busquedas else None,
                    lista_rangos=lista_rangos if lista_rangos else None
                )
                
                if resultado_busqueda.get('error', False):
                    print(f"Error al buscar registros para eliminar: {resultado_busqueda.get('message', 'Error desconocido')}")
                    return resultado_busqueda
                
                records_to_delete = resultado_busqueda.get('numeros_registro', [])
                
                if not records_to_delete:
                    print("No se encontraron registros que cumplan las condiciones para eliminar")
                    return {
                        'error': False,
                        'table_name': table_name,
                        'records_deleted': [],
                        'count': 0,
                        'message': 'No se encontraron registros para eliminar'
                    }
                
                print(f"Se encontraron {len(records_to_delete)} registros para eliminar: {records_to_delete}")
                
                # Eliminar los registros usando el método delete_records del storage manager
                deleted_count = storage_manager.delete_records(records_to_delete)
                
                success_message = f'Se eliminaron {deleted_count} registro(s) exitosamente'
                print(f"✅ {success_message}")
                
                return {
                    'error': False,
                    'table_name': table_name,
                    'lista_busquedas': lista_busquedas,
                    'lista_rangos': lista_rangos,
                    'records_deleted': records_to_delete,
                    'count': deleted_count,
                    'message': success_message
                }
                
            except Exception as e:
                error_result = {
                    'error': True,
                    'message': f"Error al ejecutar DELETE en tabla '{table_name}': {str(e)}"
                }
                print(f"Error: {error_result['message']}")
                return error_result
        else:
            error_result = {
                'error': True,
                'message': f"No hay un gestor de almacenamiento para la tabla '{table_name}'"
            }
            print(f"Error: {error_result['message']}")
            return error_result
    
    def _process_import_csv(self, sql_statement):
        """
        Procesa una instrucción IMPORT FROM CSV y la convierte en inserciones.
        
        Args:
            sql_statement (str): La instrucción SQL IMPORT FROM CSV.
            
        Returns:
            dict: Información del import procesado, similar a _process_insert.
        """
        import_info = self.parse_sql_import_csv(sql_statement)
        
        # Si hubo error en el parsing, retornarlo directamente
        if import_info.get('error', False):
            print(f"Error en IMPORT FROM CSV: {import_info['message']}")
            return import_info
        
        table_name = import_info['table_name']
        records = import_info['records']  # Lista de diccionarios como en INSERT
        
        # Verificar que la tabla existe
        if table_name not in self.tables:
            error_result = {
                'error': True,
                'message': f"La tabla '{table_name}' no existe"
            }
            print(f"Error: {error_result['message']}")
            return error_result
        
        # Si hay un gestor de almacenamiento para esta tabla, insertar los registros
        if table_name in self.storage_managers:
            storage_manager = self.storage_managers[table_name]
            inserted_ids = []
            failed_inserts = []
            
            print(f"📥 Insertando {len(records)} registros desde CSV...")
            
            for i, record in enumerate(records, 1):
                try:
                    record_id = storage_manager.insert(record)
                    if record_id:
                        inserted_ids.append(record_id)
                        print(f"   ✅ Registro {i}: ID {record_id}")
                    else:
                        failed_inserts.append(i)
                        print(f"   ❌ Registro {i}: No se pudo insertar (clave duplicada?)")
                except Exception as e:
                    failed_inserts.append(i)
                    print(f"   ❌ Registro {i}: Error - {str(e)}")
            
            success_count = len(inserted_ids)
            fail_count = len(failed_inserts)
            
            print(f"📊 Resultado: {success_count} exitosos, {fail_count} fallidos")
            
            return {
                'error': False,
                'table_name': table_name,
                'csv_file': import_info['csv_file'],
                'records': records,
                'inserted_ids': inserted_ids,
                'failed_inserts': failed_inserts,
                'total_records': len(records),
                'successful_inserts': success_count,
                'failed_inserts_count': fail_count
            }
        else:
            error_result = {
                'error': True,
                'message': f"No hay un gestor de almacenamiento para la tabla '{table_name}'"
            }
            print(f"Error: {error_result['message']}")
            return error_result
    
    def parse_sql_import_csv(self, sql_statement):
        """
        Analiza una instrucción SQL IMPORT FROM CSV y lee el archivo CSV.
        Convierte el CSV en una lista de records (diccionarios) igual que parse_sql_insert.
        
        Formato soportado:
        IMPORT FROM CSV 'ruta/archivo.csv' INTO tabla_name;
        IMPORT FROM CSV 'ruta/archivo.csv' INTO tabla_name WITH DELIMITER ';';
        
        Args:
            sql_statement (str): La instrucción SQL IMPORT FROM CSV.
            
        Returns:
            dict: Información extraída con records en formato lista de diccionarios
        """
        
        # Patrón para IMPORT FROM CSV
        import_pattern = re.compile(
            r"IMPORT\s+FROM\s+CSV\s+'([^']+)'\s+INTO\s+(\w+)(?:\s+WITH\s+(.+?))?(?:\s*;)?$",
            re.IGNORECASE
        )
        
        match = import_pattern.match(sql_statement.strip())
        if not match:
            return {
                'error': True,
                'message': "Formato de IMPORT FROM CSV no válido. Use: IMPORT FROM CSV 'archivo.csv' INTO tabla;"
            }
        
        csv_file_path = match.group(1)
        table_name = match.group(2)
        options_str = match.group(3)
        
        print(f"📥 PROCESANDO: IMPORT FROM CSV '{csv_file_path}' INTO {table_name}")
        
        # Verificar que la tabla existe
        if table_name not in self.tables:
            return {
                'error': True,
                'message': f"La tabla '{table_name}' no existe"
            }
        
        # Verificar que el archivo existe
        if not os.path.exists(csv_file_path):
            return {
                'error': True,
                'message': f"El archivo CSV '{csv_file_path}' no existe"
            }
        
        # Parsear opciones si existen
        delimiter = ','
        encoding = 'utf-8'
        skip_header = True
        
        if options_str:
            if 'DELIMITER' in options_str.upper():
                delimiter_match = re.search(r"DELIMITER\s*['\"]([^'\"]+)['\"]", options_str, re.IGNORECASE)
                if delimiter_match:
                    delimiter = delimiter_match.group(1)
            
            if 'ENCODING' in options_str.upper():
                encoding_match = re.search(r"ENCODING\s*['\"]([^'\"]+)['\"]", options_str, re.IGNORECASE)
                if encoding_match:
                    encoding = encoding_match.group(1)
            
            if 'NO_HEADER' in options_str.upper():
                skip_header = False
        
        table_info = self.tables[table_name]
        
        try:
            # Leer el archivo CSV
            with open(csv_file_path, 'r', encoding=encoding, newline='') as csvfile:
                # Auto-detectar dialecto si es necesario
                sample = csvfile.read(1024)
                csvfile.seek(0)
                
                try:
                    sniffer = csv.Sniffer()
                    dialect = sniffer.sniff(sample, delimiters=delimiter + ';\t|')
                    if hasattr(dialect, 'delimiter'):
                        delimiter = dialect.delimiter
                except:
                    pass  # Usar delimiter especificado
                
                print(f"   📋 Delimitador: '{delimiter}', Encoding: {encoding}")
                
                reader = csv.reader(csvfile, delimiter=delimiter)
                rows = list(reader)
                
                if not rows:
                    return {
                        'error': True,
                        'message': 'El archivo CSV está vacío'
                    }
                
                # Procesar headers
                headers = None
                data_rows = rows
                
                if skip_header and len(rows) > 0:
                    headers = [col.strip() for col in rows[0]]
                    data_rows = rows[1:]
                    print(f"   📄 Headers: {headers}")
                else:
                    # Si no hay header, usar nombres de columnas de la tabla en orden
                    headers = [attr['name'] for attr in table_info['attributes']]
                    print(f"   📄 Sin headers, usando: {headers}")
                
                print(f"   📊 Filas de datos: {len(data_rows)}")
                
                # Crear mapeo de columnas CSV a atributos de tabla
                column_mapping = self._create_csv_column_mapping(table_info, headers)
                
                if not column_mapping:
                    return {
                        'error': True,
                        'message': 'No se pudo mapear las columnas del CSV a la tabla'
                    }
                
                print(f"   🗺️ Mapeo: {column_mapping}")
                
                # Convertir filas CSV a records (lista de diccionarios)
                records = []
                primary_key_attr = table_info.get('primary_key')
                
                for row_num, row in enumerate(data_rows, 1):
                    try:
                        record = {}
                        
                        # Inicializar todos los atributos con valores por defecto
                        for attr in table_info['attributes']:
                            attr_name = attr['name']
                            default_value = self._get_default_value_for_type(attr['data_type'])
                            record[attr_name] = default_value
                        
                        # Mapear cada columna del CSV que tengamos
                        for csv_col_index, table_attr in column_mapping.items():
                            if csv_col_index < len(row):
                                value_str = row[csv_col_index].strip() if row[csv_col_index] else None
                                
                                # Si el valor no está vacío, convertirlo
                                if value_str and value_str.lower() not in ['', 'null', 'none', 'n/a', 'na']:
                                    converted_value = self._convert_csv_value(value_str, table_attr, table_info)
                                    if converted_value is not None:
                                        record[table_attr] = converted_value
                                # Si está vacío, ya tiene el valor por defecto
                        
                        # Validar que el primary key no esté vacío (debe ser un valor real, no el default)
                        if primary_key_attr:
                            pk_value = record.get(primary_key_attr)
                            default_pk = self._get_default_value_for_type(
                                next(attr['data_type'] for attr in table_info['attributes'] if attr['name'] == primary_key_attr)
                            )
                            
                            # Si el PK sigue siendo el valor por defecto, verificar si vino del CSV
                            pk_col_in_csv = False
                            for csv_col_index, table_attr in column_mapping.items():
                                if table_attr == primary_key_attr and csv_col_index < len(row):
                                    csv_value = row[csv_col_index].strip() if row[csv_col_index] else None
                                    if csv_value and csv_value.lower() not in ['', 'null', 'none', 'n/a', 'na']:
                                        pk_col_in_csv = True
                                    break
                            
                            if not pk_col_in_csv or pk_value == default_pk:
                                print(f"   ❌ Fila {row_num}: Primary key vacío o inválido, saltando")
                                continue
                        
                        records.append(record)
                        print(f"   ✅ Fila {row_num}: {record}")
                
                    except Exception as e:
                        print(f"   ❌ Fila {row_num}: Error - {str(e)}")
                        continue
                
                if not records:
                    return {
                        'error': True,
                        'message': 'No se pudieron convertir registros válidos del CSV'
                    }
                
                print(f"   🎯 Total de registros válidos: {len(records)}")
                
                return {
                    'error': False,
                    'table_name': table_name,
                    'csv_file': csv_file_path,
                    'records': records,  # Lista de diccionarios igual que parse_sql_insert
                    'total_rows_in_csv': len(data_rows),
                    'valid_records': len(records)
                }
                
        except Exception as e:
            return {
                'error': True,
                'message': f"Error al procesar archivo CSV: {str(e)}"
            }
    
    def _create_csv_column_mapping(self, table_info, csv_headers):
        """
        Crea mapeo automático entre headers CSV y atributos de tabla.
        
        Returns:
            dict: {indice_csv: nombre_atributo_tabla}
        """
        mapping = {}
        table_attributes = {attr['name'].lower(): attr['name'] for attr in table_info['attributes']}
        
        for i, header in enumerate(csv_headers):
            header_clean = header.lower().strip()
            
            # Coincidencia exacta
            if header_clean in table_attributes:
                mapping[i] = table_attributes[header_clean]
                continue
            
            # Coincidencia parcial
            for table_attr_lower, table_attr_original in table_attributes.items():
                if (header_clean in table_attr_lower or 
                    table_attr_lower in header_clean or
                    header_clean.replace('_', '').replace(' ', '') == table_attr_lower.replace('_', '')):
                    mapping[i] = table_attr_original
                    break
        
        return mapping
    
    def _convert_csv_value(self, value_str, attr_name, table_info):
        """
        Convierte valor CSV al tipo correcto según la tabla.
        VERSIÓN ACTUALIZADA que maneja tipo POINT.
        """
        # Buscar tipo de dato del atributo
        data_type = None
        for attr in table_info['attributes']:
            if attr['name'] == attr_name:
                data_type = attr['data_type'].upper()
                break
        
        if not data_type:
            return value_str
        
        try:
            if data_type == 'POINT':
                # Intentar convertir string a Point
                # Formatos soportados: "(1.5, 2.3)", "1.5, 2.3", "1.5;2.3", etc.
                return Point.from_string(value_str)
            elif 'INT' in data_type:
                return int(float(value_str))  # float primero por si hay decimales
            elif 'DECIMAL' in data_type or 'FLOAT' in data_type:
                return float(value_str)
            elif 'BOOL' in data_type:
                return value_str.lower() in ('true', 'yes', '1', 't', 'y', 'sí')
            else:
                return str(value_str)
        except (ValueError, TypeError):
            print(f"     ⚠️ No se pudo convertir '{value_str}' a {data_type}")
            # En lugar de retornar None, retornar valor por defecto
            return self._get_default_value_for_type(data_type)
    
    def _get_default_value_for_type(self, data_type):
        """
        Obtiene el valor por defecto para un tipo de dato cuando el campo está vacío.
        VERSIÓN ACTUALIZADA que maneja tipo POINT.
        
        Args:
            data_type (str): Tipo de dato del atributo
            
        Returns:
            Valor por defecto según el tipo
        """
        data_type_upper = data_type.upper()
        
        if data_type_upper == 'POINT':
            return Point(0.0, 0.0)  # Punto origen como valor por defecto
        elif 'INT' in data_type_upper:
            return 0
        elif 'DECIMAL' in data_type_upper or 'FLOAT' in data_type_upper or 'DOUBLE' in data_type_upper:
            return 0.0
        elif 'BOOL' in data_type_upper:
            return False
        elif 'DATE' in data_type_upper:
            return 0  # Timestamp 0 = 1970-01-01
        else:
            # Para VARCHAR, CHAR, etc.
            return " "  # Un espacio como solicitado
    
    def parse_sql_create_table(self, sql_statement):
        """
        Analiza una instrucción SQL CREATE TABLE para extraer información sobre la tabla,
        sus atributos y tipos de datos, incluyendo información sobre claves primarias e índices.
        
        Args:
            sql_statement (str): La instrucción SQL CREATE TABLE.
            
        Returns:
            dict: Un diccionario que contiene la información extraída, o None si no es válida.
        """
        # Expresión regular para encontrar el nombre de la tabla
        table_name_pattern = re.compile(r"CREATE\s+TABLE\s+([A-Za-z_\d]+)\s*\(", re.IGNORECASE)
        
        # Expresión regular mejorada para capturar atributos con sus tipos, claves e índices
        attribute_pattern = re.compile(
            r"\s*(\w+)\s+([A-Za-z_\d\[\]]+)"        # Nombre y tipo de dato (como VARCHAR[50] o POINT)
            r"(?:\s+(PRIMARY\s+KEY|KEY))?"          # PRIMARY KEY o KEY
            r"(?:\s+INDEX\s+(\w+))?"                # INDEX con tipo (como BTree)
            r"(?:\s+SEQ)?"                          # SEQ opcional
            r"\s*(?:,|$)",                          # Coma final o fin de línea
            re.IGNORECASE
        )
        
        # Buscar el nombre de la tabla
        table_name_match = table_name_pattern.search(sql_statement)
        if not table_name_match:
            return None
        
        table_name = table_name_match.group(1)
        
        # Extraer el contenido entre paréntesis
        content_match = re.search(r'\(\s*(.*?)\s*\)', sql_statement, re.DOTALL)
        if not content_match:
            return None
            
        content = content_match.group(1)
        
        # Buscar los atributos
        attributes = []
        primary_key = None
        
        attribute_matches = attribute_pattern.finditer(content)
        for match in attribute_matches:
            attribute_name = match.group(1)
            attribute_data_type = match.group(2)
            is_key = match.group(3) if match.group(3) else None
            index_type = match.group(4).lower() if match.group(4) else "hash"  # Por defecto: hash
            
            attribute = {
                "name": attribute_name,
                "data_type": attribute_data_type,
                "is_key": False,
                "index": index_type
            }
            
            # Verificar si es clave primaria
            if is_key and is_key.upper() in ["PRIMARY KEY", "KEY"]:
                attribute["is_key"] = True
                primary_key = attribute_name
            
            attributes.append(attribute)
        
        # Crear el diccionario con la información extraída
        table_info = {
            "table_name": table_name,
            "attributes": attributes,
            "primary_key": primary_key
        }
        
        return table_info
    
    def parse_sql_insert(self, sql_statement):
        """
        Analiza una instrucción SQL INSERT INTO para extraer el nombre de la tabla
        y los valores a insertar.
        VERSIÓN ACTUALIZADA que maneja tipo POINT.
        
        Args:
            sql_statement (str): La instrucción SQL INSERT INTO.
            
        Returns:
            dict: Un diccionario con la información extraída, o None si no es válida.
        """
        # Patrones para analizar la sentencia INSERT
        table_pattern = re.compile(r"INSERT\s+INTO\s+(\w+)\s*(?:\((.*?)\))?\s*VALUES\s*", re.IGNORECASE | re.DOTALL)
        values_pattern = re.compile(r"VALUES\s*\((.*?)\)(?:\s*,\s*\((.*?)\))*", re.IGNORECASE | re.DOTALL)
        
        # Buscar nombre de la tabla y columnas (opcional)
        table_match = table_pattern.search(sql_statement)
        if not table_match:
            return None
        
        table_name = table_match.group(1)
        columns_str = table_match.group(2)
        
        # Si las columnas no están especificadas, usar todas las columnas de la tabla
        columns = []
        if columns_str:
            columns = [col.strip() for col in columns_str.split(',')]
        else:
            # Si no se especificaron columnas, usar las de la definición de la tabla
            if table_name in self.tables:
                columns = [attr['name'] for attr in self.tables[table_name]['attributes']]
        
        # Buscar los valores a insertar
        values_match = values_pattern.search(sql_statement)
        if not values_match:
            return None
        
        # Extraer todos los conjuntos de valores
        value_sets = []
        
        # Primero, extraer todo lo que sigue a VALUES
        values_part = sql_statement[values_match.start():]
        
        # Función auxiliar para dividir por comas teniendo en cuenta paréntesis y strings
        def split_values(values_str):
            result = []
            current = ""
            depth = 0
            in_string = False
            string_char = None
            
            for char in values_str:
                if char in ["'", '"'] and (not in_string or string_char == char):
                    in_string = not in_string
                    if in_string:
                        string_char = char
                    else:
                        string_char = None
                
                if char == '(' and not in_string:
                    depth += 1
                    if depth == 1:  # Inicio de un conjunto de valores
                        current = ""
                        continue
                elif char == ')' and not in_string:
                    depth -= 1
                    if depth == 0:  # Fin de un conjunto de valores
                        result.append(current.strip())
                        continue
                
                if depth > 0:
                    current += char
            
            return result
        
        # Extraer todos los conjuntos de valores
        value_sets_str = split_values(values_part)
        
        records = []
        for value_set_str in value_sets_str:
            # Analizar cada valor del conjunto
            values = self._parse_values(value_set_str)
            
            # Asociar cada valor con su columna correspondiente
            record = {}
            for i, value in enumerate(values):
                if i < len(columns):
                    column_name = columns[i]
                    # Convertir el valor según el tipo de dato de la columna
                    record[column_name] = self._convert_value(value, table_name, column_name)
            
            records.append(record)
        
        return {
            'table_name': table_name,
            'columns': columns,
            'records': records
        }
    
    def parse_sql_select(self, sql_statement):
        """
        Analiza una instrucción SQL SELECT y extrae las condiciones de búsqueda.
        Versión actualizada que soporta BETWEEN para rangos y atributos específicos.
        Soporta:
        - SELECT * FROM tabla WHERE attr=value [AND attr2=value2 ...]
        - SELECT attr1, attr2 FROM tabla WHERE attr=value
        - SELECT * FROM tabla WHERE attr BETWEEN min_val AND max_val
        - SELECT attr1 FROM tabla WHERE attr=value AND attr2 BETWEEN min_val AND max_val
        
        Args:
            sql_statement (str): La instrucción SQL SELECT.
            
        Returns:
            dict: Información extraída con lista_busquedas, lista_rangos y atributos solicitados
        """
        # Patrón para SELECT básico
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
        where_clause = match.group(3)
        
        # Verificar que la tabla existe
        if table_name not in self.tables:
            return {
                "error": True,
                "message": f"La tabla '{table_name}' no existe"
            }
        
        # Procesar columnas solicitadas
        requested_attributes = []
        if columns_str == "*":
            # Seleccionar todos los atributos
            requested_attributes = [attr['name'] for attr in self.tables[table_name]['attributes']]
        else:
            # Parsear atributos específicos
            attr_names = [attr.strip() for attr in columns_str.split(',')]
            
            # Verificar que todos los atributos existen en la tabla
            table_attributes = {attr['name'] for attr in self.tables[table_name]['attributes']}
            
            for attr_name in attr_names:
                if attr_name not in table_attributes:
                    return {
                        "error": True,
                        "message": f"El atributo '{attr_name}' no existe en la tabla '{table_name}'"
                    }
            
            requested_attributes = attr_names
        
        # Analizar condiciones WHERE y separar en búsquedas exactas y rangos
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
        
        return {
            'error': False,
            'table_name': table_name,
            'columns': columns_str,
            'requested_attributes': requested_attributes,
            'lista_busquedas': lista_busquedas,
            'lista_rangos': lista_rangos
        }
    
    def parse_sql_delete(self, sql_statement):
        """
        Analiza una instrucción SQL DELETE FROM y extrae las condiciones.
        Soporta:
        - DELETE FROM tabla WHERE attr=value [AND attr2=value2 ...]
        - DELETE FROM tabla WHERE attr BETWEEN min_val AND max_val
        - DELETE FROM tabla WHERE attr=value AND attr2 BETWEEN min_val AND max_val
        
        Args:
            sql_statement (str): La instrucción SQL DELETE FROM.
            
        Returns:
            dict: Información extraída con lista_busquedas y lista_rangos
        """
        # Patrón para DELETE básico
        delete_pattern = re.compile(
            r"DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s*;)?$", 
            re.IGNORECASE | re.DOTALL
        )
        
        match = delete_pattern.match(sql_statement.strip())
        if not match:
            return {
                "error": True,
                "message": "Formato de DELETE no válido. Use: DELETE FROM tabla [WHERE condiciones]"
            }
        
        table_name = match.group(1)
        where_clause = match.group(2)
        
        # Verificar que la tabla existe
        if table_name not in self.tables:
            return {
                "error": True,
                "message": f"La tabla '{table_name}' no existe"
            }
        
        # Si no hay WHERE clause, es un DELETE sin condiciones (eliminar todo)
        if not where_clause:
            return {
                "error": True,
                "message": "DELETE sin WHERE no está permitido por seguridad. Especifique condiciones WHERE."
            }
        
        # Analizar condiciones WHERE y separar en búsquedas exactas y rangos
        lista_busquedas = []
        lista_rangos = []
        
        try:
            lista_busquedas, lista_rangos = self._parse_where_with_ranges(where_clause, table_name)
        except Exception as e:
            return {
                "error": True,
                "message": f"Error al parsear condiciones WHERE: {str(e)}"
            }
        
        return {
            'error': False,
            'table_name': table_name,
            'lista_busquedas': lista_busquedas,
            'lista_rangos': lista_rangos
        }
        
    def _parse_where_with_ranges(self, where_clause, table_name):
        """
        Analiza una cláusula WHERE y la separa en búsquedas exactas y por rango.
        VERSIÓN ACTUALIZADA que soporta operadores de comparación: >, <, >=, <=
        Y maneja tipo POINT correctamente.
        
        Soporta:
        - attr=value
        - attr BETWEEN min_val AND max_val
        - attr > value (convertido a rango)
        - attr < value (convertido a rango)
        - attr >= value (convertido a rango)
        - attr <= value (convertido a rango)
        - Combinaciones con AND
        
        Args:
            where_clause (str): La cláusula WHERE sin la palabra WHERE.
            table_name (str): Nombre de la tabla para conversión de tipos.
            
        Returns:
            tuple: (lista_busquedas, lista_rangos)
        """
        lista_busquedas = []
        lista_rangos = []
        
        print(f"Analizando WHERE clause: '{where_clause}'")
        
        # 1. Primero extraer condiciones BETWEEN
        between_pattern = r'(\w+)\s+BETWEEN\s+([^\s]+(?:\s+[^\s]+)*?)\s+AND\s+([^\s]+(?:\s+[^\s]+)*?)(?:\s+AND\s+|$)'
        between_matches = list(re.finditer(between_pattern, where_clause, re.IGNORECASE))
        
        remaining_clause = where_clause
        
        # Procesar condiciones BETWEEN
        for match in reversed(between_matches):
            attr_name = match.group(1)
            min_val_str = match.group(2).strip()
            max_val_str = match.group(3).strip()
            
            min_val = self._convert_value(min_val_str, table_name, attr_name)
            max_val = self._convert_value(max_val_str, table_name, attr_name)
            
            lista_rangos.append([attr_name, min_val, max_val])
            print(f"BETWEEN encontrado: {attr_name} BETWEEN {min_val} AND {max_val}")
            
            # Remover del remaining_clause
            remaining_clause = remaining_clause[:match.start()] + remaining_clause[match.end():]
        
        # 2. Extraer operadores de comparación (>=, <=, >, <)
        comparison_patterns = [
            (r'(\w+)\s*>=\s*([^\s]+(?:\s+[^\s]+)*?)(?:\s+AND\s+|$)', '>='),
            (r'(\w+)\s*<=\s*([^\s]+(?:\s+[^\s]+)*?)(?:\s+AND\s+|$)', '<='),
            (r'(\w+)\s*>\s*([^\s]+(?:\s+[^\s]+)*?)(?:\s+AND\s+|$)', '>'),
            (r'(\w+)\s*<\s*([^\s]+(?:\s+[^\s]+)*?)(?:\s+AND\s+|$)', '<')
        ]
        
        for pattern, operator in comparison_patterns:
            matches = list(re.finditer(pattern, remaining_clause, re.IGNORECASE))
            
            for match in reversed(matches):
                attr_name = match.group(1)
                value_str = match.group(2).strip()
                value = self._convert_value(value_str, table_name, attr_name)
                
                # Convertir operador de comparación a rango
                range_result = self._comparison_to_range(attr_name, operator, value, table_name)
                if range_result:
                    lista_rangos.append(range_result)
                    print(f"Comparación encontrada: {attr_name} {operator} {value} → rango")
                
                # Remover del remaining_clause
                remaining_clause = remaining_clause[:match.start()] + remaining_clause[match.end():]
        
        # 3. Limpiar remaining_clause
        remaining_clause = re.sub(r'\s+AND\s+AND\s+', ' AND ', remaining_clause, flags=re.IGNORECASE)
        remaining_clause = re.sub(r'^\s*AND\s+', '', remaining_clause, flags=re.IGNORECASE)
        remaining_clause = re.sub(r'\s+AND\s*$', '', remaining_clause, flags=re.IGNORECASE)
        remaining_clause = remaining_clause.strip()
        
        # 4. Procesar condiciones de igualdad restantes
        if remaining_clause:
            and_parts = re.split(r'\s+AND\s+', remaining_clause, flags=re.IGNORECASE)
            
            for part in and_parts:
                part = part.strip()
                if not part:
                    continue
                    
                # Buscar condiciones de igualdad
                eq_match = re.match(r'(\w+)\s*=\s*(.+)', part)
                if eq_match:
                    attr_name = eq_match.group(1)
                    value_str = eq_match.group(2).strip()
                    value = self._convert_value(value_str, table_name, attr_name)
                    lista_busquedas.append([attr_name, value])
                    print(f"Condición exacta: {attr_name} = {value}")
                else:
                    print(f"Advertencia: Condición no soportada: {part}")
        
        print(f"Resultado final - Búsquedas exactas: {lista_busquedas}, Rangos: {lista_rangos}")
        return lista_busquedas, lista_rangos

    def _comparison_to_range(self, attr_name, operator, value, table_name):
        """
        Convierte un operador de comparación a un rango para búsqueda.
        VERSIÓN ACTUALIZADA que maneja tipo POINT.
        
        Args:
            attr_name (str): Nombre del atributo
            operator (str): Operador (>, <, >=, <=)
            value: Valor a comparar
            table_name (str): Nombre de la tabla
            
        Returns:
            list: [attr_name, min_val, max_val] o None si no se puede convertir
        """
        # Determinar el tipo de dato para establecer límites
        data_type = self._get_attribute_data_type(table_name, attr_name)
        
        if operator == '>':
            # attr > value → rango desde (value + epsilon) hasta infinito
            if isinstance(value, (int, float)):
                epsilon = 0.01 if isinstance(value, float) else 1
                min_val = value + epsilon
                max_val = self._get_max_value_for_type(data_type)
                return [attr_name, min_val, max_val]
            elif isinstance(value, Point):
                # Para Point, usar distancia al origen + epsilon
                min_val = Point(value.x + 0.01, value.y + 0.01)
                max_val = self._get_max_value_for_type(data_type)
                return [attr_name, min_val, max_val]
        
        elif operator == '>=':
            # attr >= value → rango desde value hasta infinito
            min_val = value
            max_val = self._get_max_value_for_type(data_type)
            return [attr_name, min_val, max_val]
        
        elif operator == '<':
            if isinstance(value, (int, float)):
                epsilon = 0.01 if isinstance(value, float) else 1
                min_val = self._get_min_value_for_type(data_type)
                max_val = value - epsilon
                return [attr_name, min_val, max_val]
            elif isinstance(value, Point):
                min_val = self._get_min_value_for_type(data_type)
                max_val = Point(value.x - 0.01, value.y - 0.01)
                return [attr_name, min_val, max_val]
        
        elif operator == '<=':
            # attr <= value → rango desde -infinito hasta value
            min_val = self._get_min_value_for_type(data_type)
            max_val = value
            return [attr_name, min_val, max_val]
        
        return None

    def _get_attribute_data_type(self, table_name, attr_name):
        """
        Obtiene el tipo de dato de un atributo.
        """
        if table_name not in self.tables:
            return 'UNKNOWN'
        
        for attr in self.tables[table_name]['attributes']:
            if attr['name'] == attr_name:
                return attr['data_type'].upper()
        
        return 'UNKNOWN'

    def _get_max_value_for_type(self, data_type):
        """
        Obtiene el valor máximo para un tipo de dato.
        VERSIÓN ACTUALIZADA que maneja tipo POINT.
        """
        if data_type == 'POINT':
            return Point(999999.0, 999999.0)  # Punto máximo
        elif 'INT' in data_type:
            return 2147483647  # INT máximo
        elif 'DECIMAL' in data_type or 'FLOAT' in data_type:
            return 999999999.99  # Decimal grande
        elif 'VARCHAR' in data_type or 'CHAR' in data_type:
            return 'ZZZZZZZZZ'  # String máximo lexicográfico
        else:
            return 999999999

    def _get_min_value_for_type(self, data_type):
        """
        Obtiene el valor mínimo para un tipo de dato.
        VERSIÓN ACTUALIZADA que maneja tipo POINT.
        """
        if data_type == 'POINT':
            return Point(-999999.0, -999999.0)  # Punto mínimo
        elif 'INT' in data_type:
            return -2147483648  # INT mínimo
        elif 'DECIMAL' in data_type or 'FLOAT' in data_type:
            return -999999999.99  # Decimal pequeño
        elif 'VARCHAR' in data_type or 'CHAR' in data_type:
            return ''  # String vacío
        else:
            return -999999999
    
    def _parse_values(self, values_str):
        """
        Divide una cadena de valores separados por comas, respetando strings con comillas.
        
        Args:
            values_str (str): Cadena con valores separados por comas.
            
        Returns:
            list: Lista de valores extraídos.
        """
        values = []
        current = ""
        in_string = False
        string_char = None
        
        for char in values_str:
            if char in ["'", '"'] and (not in_string or string_char == char):
                in_string = not in_string
                if in_string:
                    string_char = char
                else:
                    string_char = None
                current += char
            elif char == ',' and not in_string:
                values.append(current.strip())
                current = ""
            else:
                current += char
        
        if current.strip():
            values.append(current.strip())
        
        return values
    
    def _convert_value(self, value_str, table_name, column_name):
        """
        Convierte un valor de string al tipo adecuado según la definición de la columna.
        VERSIÓN ACTUALIZADA que maneja tipo POINT.
        
        Args:
            value_str (str): Valor como string.
            table_name (str): Nombre de la tabla.
            column_name (str): Nombre de la columna.
            
        Returns:
            El valor convertido al tipo adecuado.
        """
        # Si la tabla no existe, devolver el valor tal cual
        if table_name not in self.tables:
            return value_str
        
        # Buscar el tipo de dato de la columna
        data_type = None
        for attr in self.tables[table_name]['attributes']:
            if attr['name'] == column_name:
                data_type = attr['data_type'].upper()
                break
        
        if not data_type:
            return value_str
        
        # Eliminar comillas si es un string
        if value_str.startswith(("'", '"')) and value_str.endswith(("'", '"')):
            value_str = value_str[1:-1]
        
        # Convertir según el tipo de dato
        try:
            if data_type == 'POINT':
                # Convertir string a Point
                return Point.from_string(value_str)
            elif 'INT' in data_type:
                return int(value_str)
            elif 'DECIMAL' in data_type or 'FLOAT' in data_type or 'DOUBLE' in data_type:
                return float(value_str)
            elif 'BOOL' in data_type:
                return value_str.lower() in ('true', 'yes', '1', 't', 'y')
            else:
                # Para VARCHAR, CHAR, etc., devolver como string
                return value_str
        except (ValueError, TypeError):
            # Si hay un error de conversión, devolver el valor original
            return value_str
    
    # Resto de métodos siguen siendo los mismos...
    def get_table(self, table_name):
        """
        Obtiene la información de una tabla específica.
        
        Args:
            table_name (str): Nombre de la tabla.
            
        Returns:
            dict: Información de la tabla o None si no existe.
        """
        return self.tables.get(table_name)
    
    def get_storage_manager(self, table_name):
        """
        Obtiene el gestor de almacenamiento de una tabla.
        
        Args:
            table_name (str): Nombre de la tabla.
            
        Returns:
            TableStorageManager: El gestor de almacenamiento o None si no existe.
        """
        return self.storage_managers.get(table_name)
    
    def get_all_tables(self):
        """
        Obtiene todas las tablas almacenadas.
        
        Returns:
            dict: Diccionario con todas las tablas.
        """
        return self.tables
    
    def execute_sql(self, sql_statement):
        """
        Ejecuta una instrucción SQL.
        
        Args:
            sql_statement (str): La instrucción SQL a ejecutar.
            
        Returns:
            list: Resultados de la ejecución.
        """
        return self.parse_sql_statement(sql_statement)
    
    def execute_select(self, sql_select_statement):
        """
        Ejecuta una consulta SELECT directamente y retorna solo los números de registro.
        
        Args:
            sql_select_statement (str): La instrucción SELECT.
            
        Returns:
            list: Lista de números de registro encontrados.
        """
        operations = self.parse_sql_statement(sql_select_statement)
        
        for op_type, result in operations:
            if op_type == "SELECT":
                if result.get('error', False):
                    return []
                resultado = result.get('resultado', {})
                if not resultado.get('error', False):
                    return resultado.get('numeros_registro', [])
        
        return []
    
    def execute_select_safe(self, sql_select_statement):
        """
        Ejecuta una consulta SELECT y retorna el resultado completo con manejo de errores.
        
        Args:
            sql_select_statement (str): La instrucción SELECT.
            
        Returns:
            dict: Resultado con información de éxito/error
        """
        try:
            operations = self.parse_sql_statement(sql_select_statement)
            
            for op_type, result in operations:
                if op_type == "SELECT":
                    return result.get('resultado', {"error": True, "message": "No se pudo obtener resultado"})
            
            return {"error": True, "message": "No se encontró operación SELECT"}
            
        except Exception as e:
            return {"error": True, "message": f"Error al ejecutar SELECT: {str(e)}"}
    
    def execute_delete(self, sql_delete_statement):
        """
        Ejecuta una consulta DELETE directamente y retorna el resultado.
        
        Args:
            sql_delete_statement (str): La instrucción DELETE.
            
        Returns:
            dict: Resultado con información de éxito/error y cantidad eliminada
        """
        try:
            operations = self.parse_sql_statement(sql_delete_statement)
            
            for op_type, result in operations:
                if op_type == "DELETE":
                    return result
            
            return {"error": True, "message": "No se encontró operación DELETE"}
            
        except Exception as e:
            return {"error": True, "message": f"Error al ejecutar DELETE: {str(e)}"}
    
   
    def display_table_info(self, table_name=None):
        """
        Muestra la información de una tabla específica o de todas las tablas.
        
        Args:
            table_name (str, opcional): Nombre de la tabla. Si es None, muestra todas las tablas.
        """
        if table_name:
            table_info = self.get_table(table_name)
            if table_info:
                self._print_table_info(table_info)
            else:
                print(f"La tabla '{table_name}' no existe.")
        else:
            if not self.tables:
                print("No hay tablas almacenadas.")
                return
                
            for table_name, table_info in self.tables.items():
                self._print_table_info(table_info)
                print("-" * 50)
    
    def _print_table_info(self, table_info):
        """
        Método auxiliar para imprimir la información de una tabla.
        
        Args:
            table_info (dict): Información de la tabla.
        """
        print(f"Tabla: {table_info['table_name']}")
        
        if table_info['primary_key']:
            print(f"Clave Primaria: {table_info['primary_key']}")
        else:
            print("No se ha definido una clave primaria.")
            
        print("Atributos:")
        for attribute in table_info["attributes"]:
            key_info = " (PRIMARY KEY)" if attribute['is_key'] else ""
            print(f"  - Nombre: {attribute['name']}, Tipo: {attribute['data_type']}, Índice: {attribute['index']}{key_info}")