import struct
import re
import os
import pickle
import json

class BPlusTreeNode:
    def __init__(self, is_leaf=False):
        self.keys = []  # Almacena números de registro
        self.children = []  # Solo para nodos internos
        self.is_leaf = is_leaf
        self.next = None  # Puntero al siguiente nodo hoja
        self.parent = None

class BPlusTree:
    def __init__(self, degree=4, record_format="<i50sdii", index_attr=2, table_name="Productos", is_key=False):
        self.root = BPlusTreeNode(is_leaf=True)
        self.degree = degree  # Grado máximo del árbol
        self.max_keys = degree - 1  # Máximo número de claves por nodo
        self.min_keys = (degree + 1) // 2 - 1  # Mínimo número de claves por nodo
        
        # Configuración para indexación por atributo
        self.record_format = record_format
        self.index_attr = index_attr  # El atributo a indexar
        self.table_name = table_name
        self.is_key = is_key  # Indica si el atributo es una clave (no permite duplicados)
        
        # Analizar el formato para determinar los tipos de campo
        self.field_types = self._parse_format(record_format)
        self.record_size = struct.calcsize(self.record_format)
        
        # Configurar rutas de archivos
        self.index_dir = "indices"
        self.index_filename = f"{self.table_name}_{self.index_attr}"
        self.tree_file = os.path.join(self.index_dir, f"{self.index_filename}_tree.dat")
        self.metadata_file = os.path.join(self.index_dir, f"{self.index_filename}_meta.dat")
        
        # Crear directorio si no existe
        os.makedirs(self.index_dir, exist_ok=True)
        
        # Cargar índice si existe
        self.load_index()

    def _parse_format(self, format_str):
        """Analiza el formato del registro para determinar los tipos de cada campo."""
        pattern = r'([<>]?)([a-zA-Z])(\d*)'
        matches = re.findall(pattern, format_str)
        
        field_types = []
        for _, type_char, size_str in matches:
            if type_char == 'i':
                field_types.append('int')
            elif type_char == 'd':  
                field_types.append('double')
            elif type_char == 's':
                field_types.append('string')
            elif type_char == 'c':
                field_types.append('char')
            else:
                field_types.append('unknown')
        
        return field_types

    def save_index(self):
        """Guarda el índice B+ Tree en archivos .dat"""
        try:
            # Guardar metadatos
            metadata = {
                'degree': self.degree,
                'record_format': self.record_format,
                'index_attr': self.index_attr,
                'table_name': self.table_name,
                'is_key': self.is_key,
                'field_types': self.field_types,
                'record_size': self.record_size
            }
            
            with open(self.metadata_file, 'wb') as f:
                # Usar pickle para guardar metadatos
                pickle.dump(metadata, f)
            
            # Guardar estructura del árbol
            tree_data = self._serialize_tree()
            with open(self.tree_file, 'wb') as f:
                pickle.dump(tree_data, f)
                
            print(f"Índice guardado en: {self.tree_file}")
            
        except Exception as e:
            print(f"Error al guardar índice: {e}")

    def load_index(self):
        """Carga el índice B+ Tree desde archivos .dat"""
        try:
            # Verificar si los archivos existen
            if not os.path.exists(self.metadata_file) or not os.path.exists(self.tree_file):
                print(f"No existe índice previo para {self.index_filename}")
                return False
            
            # Cargar metadatos
            with open(self.metadata_file, 'rb') as f:
                metadata = pickle.load(f)
            
            # Verificar compatibilidad de metadatos
            if (metadata['degree'] != self.degree or 
                metadata['record_format'] != self.record_format or
                metadata['index_attr'] != self.index_attr):
                print(f"Metadatos incompatibles, creando nuevo índice")
                return False
            
            # Cargar estructura del árbol
            with open(self.tree_file, 'rb') as f:
                tree_data = pickle.load(f)
            
            # Deserializar el árbol
            self._deserialize_tree(tree_data)
            print(f"Índice cargado desde: {self.tree_file}")
            return True
            
        except Exception as e:
            print(f"Error al cargar índice: {e}")
            return False

    def _serialize_tree(self):
        """Serializa la estructura del árbol para guardado"""
        if not self.root:
            return None
        
        # Obtener todas las hojas en orden
        all_leaves = []
        leaf = self._get_first_leaf()
        while leaf is not None:
            all_leaves.append(leaf.keys.copy())
            leaf = leaf.next
        
        def serialize_node(node):
            if node is None:
                return None
            
            node_data = {
                'keys': node.keys.copy(),
                'is_leaf': node.is_leaf,
                'children': []
            }
            
            # Serializar hijos recursivamente
            if not node.is_leaf:
                for child in node.children:
                    node_data['children'].append(serialize_node(child))
            
            return node_data
        
        return {
            'root': serialize_node(self.root),
            'leaf_data': all_leaves  # Guardar datos de hojas por separado
        }

    def _deserialize_tree(self, tree_data):
        """Deserializa la estructura del árbol desde archivo"""
        if tree_data is None or tree_data['root'] is None:
            self.root = BPlusTreeNode(is_leaf=True)
            return
        
        def deserialize_node(node_data, parent=None):
            if node_data is None:
                return None
            
            node = BPlusTreeNode(is_leaf=node_data['is_leaf'])
            node.keys = node_data['keys'].copy()
            node.parent = parent
            
            # Deserializar hijos
            if not node.is_leaf:
                for child_data in node_data['children']:
                    child = deserialize_node(child_data, node)
                    if child:
                        node.children.append(child)
            
            return node
        
        # Reconstruir estructura del árbol
        self.root = deserialize_node(tree_data['root'])
        
        # Reconstruir hojas y sus enlaces
        leaf_data = tree_data.get('leaf_data', [])
        if leaf_data:
            # Obtener todas las hojas del árbol reconstruido
            leaves = []
            self._collect_leaves(self.root, leaves)
            
            # Asignar datos a cada hoja y crear enlaces
            for i, leaf in enumerate(leaves):
                if i < len(leaf_data):
                    leaf.keys = leaf_data[i].copy()
                
                # Crear enlace al siguiente nodo hoja
                if i < len(leaves) - 1:
                    leaf.next = leaves[i + 1]
    
    def _collect_leaves(self, node, leaves):
        """Recolecta todas las hojas del árbol en orden de izquierda a derecha"""
        if node is None:
            return
        
        if node.is_leaf:
            leaves.append(node)
        else:
            for child in node.children:
                self._collect_leaves(child, leaves)

    def get_attribute_from_record_num(self, record_num):
        """Obtiene el valor del atributo indexado desde un número de registro en el archivo de tablas."""
        tabla_filename = f"tablas/{self.table_name}.bin"
        tabla_header_format = "<i"
        tabla_header_size = struct.calcsize(tabla_header_format)
        
        try:
            with open(tabla_filename, 'rb') as f:
                # Calcular la posición del registro
                position = tabla_header_size + (record_num - 1) * self.record_size
                f.seek(position)
                
                # Leer el registro completo
                record_data = f.read(self.record_size)
                
                if not record_data or len(record_data) < self.record_size:
                    return None
                
                # Desempaquetar los datos usando el formato
                unpacked_data = list(struct.unpack(self.record_format, record_data))
                
                # Obtener el valor del atributo indexado
                if self.index_attr < 1 or self.index_attr > len(unpacked_data):
                    return None
                    
                indexed_value = unpacked_data[self.index_attr - 1]
                
                # Convertir según el tipo de campo
                field_type = self.field_types[self.index_attr - 1] if self.index_attr - 1 < len(self.field_types) else 'unknown'
                if field_type == 'string':
                    indexed_value = indexed_value.decode('utf-8').rstrip('\0')
                
                return indexed_value
                
        except FileNotFoundError:
            return None
        except Exception as e:
            return None

    def _compare_record_values(self, record_num1, record_num2):
        """Compara dos números de registro basándose en el valor de sus atributos indexados."""
        valor1 = self.get_attribute_from_record_num(record_num1)
        valor2 = self.get_attribute_from_record_num(record_num2)
        
        if valor1 is None or valor2 is None:
            return 0
        
        # Manejar comparación entre tipos diferentes
        try:
            if valor1 < valor2:
                return -1
            elif valor1 > valor2:
                return 1
            else:
                return 0
        except TypeError:
            str1 = str(valor1)
            str2 = str(valor2)
            if str1 < str2:
                return -1
            elif str1 > str2:
                return 1
            else:
                return 0

    def _compare_value_with_record(self, target_value, record_num):
        """Compara un valor objetivo con el valor del atributo de un registro."""
        record_value = self.get_attribute_from_record_num(record_num)
        
        if record_value is None:
            return 0
        
        try:
            if target_value < record_value:
                return -1
            elif target_value > record_value:
                return 1
            else:
                return 0
        except TypeError:
            target_str = str(target_value)
            record_str = str(record_value)
            if target_str < record_str:
                return -1
            elif target_str > record_str:
                return 1
            else:
                return 0
        
    def search(self, target_value):
        """Busca registros que tengan el valor específico en el atributo indexado."""
        result = []
        leaf = self._find_leaf_by_value(target_value)
        
        # Buscar en la hoja encontrada
        self._search_in_leaf(leaf, target_value, result)
        
        # Si permite duplicados, buscar en hojas adyacentes
        if not self.is_key:
            current = leaf
            while current.next and current.next.keys:
                first_record = current.next.keys[0]
                if self._compare_value_with_record(target_value, first_record) == 0:
                    self._search_in_leaf(current.next, target_value, result)
                    current = current.next
                else:
                    break
        
        return result

    def _search_in_leaf(self, leaf, target_value, result):
        """Busca en una hoja específica todos los registros que coincidan con el valor."""
        for record_num in leaf.keys:
            if self._compare_value_with_record(target_value, record_num) == 0:
                result.append(record_num)

    def _find_leaf_by_value(self, target_value):
        """Encuentra el nodo hoja donde debería estar un valor específico del atributo."""
        node = self.root
        
        while not node.is_leaf:
            i = 0
            while i < len(node.keys):
                record_num = node.keys[i]
                if self._compare_value_with_record(target_value, record_num) < 0:
                    break
                i += 1
            node = node.children[i]
        
        return node
    
    def _find_leaf_for_record(self, record_num):
        """Encuentra el nodo hoja donde debería estar un número de registro específico."""
        record_value = self.get_attribute_from_record_num(record_num)
        if record_value is None:
            return self.root if self.root.is_leaf else self._get_first_leaf()
        
        return self._find_leaf_by_value(record_value)
    
    def insert_record(self, record_num):
        """Inserta un número de registro en el árbol."""
        # Verificar si permite duplicados
        if self.is_key:
            record_value = self.get_attribute_from_record_num(record_num)
            if record_value is not None:
                existing_records = self.search(record_value)
                if existing_records:
                    print(f"Registro {record_num} (valor: {record_value}) ya existe y no se permiten duplicados.")
                    return False
        
        leaf = self._find_leaf_for_record(record_num)
        
        # Verificar si el registro ya existe exactamente
        if record_num in leaf.keys:
            return False
        
        # Insertar en orden basándose en el valor del atributo
        self._insert_record_in_leaf(leaf, record_num)
        
        # Verificar si necesitamos dividir
        if len(leaf.keys) > self.max_keys:
            self._split_leaf(leaf)
        
        # Guardar cambios automáticamente
        self.save_index()
        return True

    def _insert_record_in_leaf(self, leaf, record_num):
        """Inserta un número de registro en una hoja manteniendo el orden por valor de atributo."""
        if not leaf.keys:
            leaf.keys.append(record_num)
            return
        
        inserted = False
        for i, existing_record in enumerate(leaf.keys):
            if self._compare_record_values(record_num, existing_record) <= 0:
                leaf.keys.insert(i, record_num)
                inserted = True
                break
        
        if not inserted:
            leaf.keys.append(record_num)
    
    def _split_leaf(self, leaf):
        """Divide un nodo hoja que está lleno"""
        mid = len(leaf.keys) // 2
        
        new_leaf = BPlusTreeNode(is_leaf=True)
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.next = leaf.next
        new_leaf.parent = leaf.parent
        
        leaf.keys = leaf.keys[:mid]
        leaf.next = new_leaf
        
        promote_record = new_leaf.keys[0]
        
        if leaf.parent is None:
            new_root = BPlusTreeNode(is_leaf=False)
            new_root.keys = [promote_record]
            new_root.children = [leaf, new_leaf]
            leaf.parent = new_root
            new_leaf.parent = new_root
            self.root = new_root
        else:
            self._insert_internal(leaf.parent, promote_record, new_leaf)
    
    def _insert_internal(self, node, record_num, right_child):
        """Inserta un número de registro en un nodo interno"""
        inserted = False
        for i, existing_record in enumerate(node.keys):
            if self._compare_record_values(record_num, existing_record) <= 0:
                node.keys.insert(i, record_num)
                node.children.insert(i + 1, right_child)
                inserted = True
                break
        
        if not inserted:
            node.keys.append(record_num)
            node.children.append(right_child)
        
        right_child.parent = node
        
        if len(node.keys) > self.max_keys:
            self._split_internal(node)
    
    def _split_internal(self, node):
        """Divide un nodo interno que está lleno"""
        mid = len(node.keys) // 2
        promote_record = node.keys[mid]
        
        new_node = BPlusTreeNode(is_leaf=False)
        new_node.keys = node.keys[mid + 1:]
        new_node.children = node.children[mid + 1:]
        new_node.parent = node.parent
        
        for child in new_node.children:
            child.parent = new_node
        
        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]
        
        if node.parent is None:
            new_root = BPlusTreeNode(is_leaf=False)
            new_root.keys = [promote_record]
            new_root.children = [node, new_node]
            node.parent = new_root
            new_node.parent = new_root
            self.root = new_root
        else:
            self._insert_internal(node.parent, promote_record, new_node)
    
    def delete(self, target_value):
        """Elimina registro(s) que tengan el valor específico del atributo."""
        records_to_delete = self.search(target_value)
        
        if not records_to_delete:
            return False
        
        for record_num in records_to_delete:
            self.delete_record(record_num)
        
        return True

    def delete_record(self, record_num):
        """Elimina un número de registro específico del árbol."""
        leaf = self._find_leaf_for_record(record_num)
        
        if record_num not in leaf.keys:
            return False
        
        leaf.keys.remove(record_num)
        
        self._update_internal_keys_after_deletion(leaf, record_num)
        
        if len(leaf.keys) < self.min_keys and leaf != self.root:
            self._rebalance_leaf(leaf)
        elif len(leaf.keys) == 0 and leaf == self.root:
            pass
        
        # Guardar cambios automáticamente
        self.save_index()
        return True
    
    def _update_internal_keys_after_deletion(self, leaf, deleted_record):
        """Actualiza las claves en nodos internos después de una eliminación"""
        if not leaf.keys:
            return
        
        self._update_separator_keys(deleted_record, leaf)
    
    def _update_separator_keys(self, old_record, leaf):
        """Actualiza las claves separadoras en los nodos internos"""
        if not leaf.parent:
            return
        
        parent = leaf.parent
        
        try:
            leaf_index = parent.children.index(leaf)
        except ValueError:
            return
        
        if old_record in parent.keys:
            key_index = parent.keys.index(old_record)
            if leaf.keys:
                parent.keys[key_index] = leaf.keys[0]
        
        self._update_separator_keys(old_record, parent)
    
    def _rebalance_leaf(self, leaf):
        """Rebalancea un nodo hoja después de una eliminación"""
        parent = leaf.parent
        if parent is None:
            return
        
        leaf_index = parent.children.index(leaf)
        
        # Intentar redistribuir con hermano izquierdo
        if leaf_index > 0:
            left_sibling = parent.children[leaf_index - 1]
            if len(left_sibling.keys) > self.min_keys:
                borrowed_record = left_sibling.keys.pop()
                leaf.keys.insert(0, borrowed_record)
                parent.keys[leaf_index - 1] = leaf.keys[0]
                return
        
        # Intentar redistribuir con hermano derecho
        if leaf_index < len(parent.children) - 1:
            right_sibling = parent.children[leaf_index + 1]
            if len(right_sibling.keys) > self.min_keys:
                borrowed_record = right_sibling.keys.pop(0)
                leaf.keys.append(borrowed_record)
                leaf.keys.sort(key=lambda x: self.get_attribute_from_record_num(x) or "")
                parent.keys[leaf_index] = right_sibling.keys[0]
                return
        
        # Fusionar con un hermano
        if leaf_index > 0:
            left_sibling = parent.children[leaf_index - 1]
            left_sibling.keys.extend(leaf.keys)
            left_sibling.keys.sort(key=lambda x: self.get_attribute_from_record_num(x) or "")
            left_sibling.next = leaf.next
            
            parent.keys.pop(leaf_index - 1)
            parent.children.pop(leaf_index)
        else:
            right_sibling = parent.children[leaf_index + 1]
            leaf.keys.extend(right_sibling.keys)
            leaf.keys.sort(key=lambda x: self.get_attribute_from_record_num(x) or "")
            leaf.next = right_sibling.next
            
            parent.keys.pop(leaf_index)
            parent.children.pop(leaf_index + 1)
        
        if len(parent.keys) < self.min_keys and parent != self.root:
            self._rebalance_internal(parent)
        elif len(parent.keys) == 0 and parent == self.root:
            if parent.children:
                self.root = parent.children[0]
                self.root.parent = None
    
    def _rebalance_internal(self, node):
        """Rebalancea un nodo interno después de una eliminación"""
        parent = node.parent
        if parent is None:
            return
        
        node_index = parent.children.index(node)
        
        if node_index > 0:
            left_sibling = parent.children[node_index - 1]
            separator = parent.keys.pop(node_index - 1)
            
            left_sibling.keys.append(separator)
            left_sibling.keys.extend(node.keys)
            left_sibling.children.extend(node.children)
            
            for child in node.children:
                child.parent = left_sibling
            
            parent.children.pop(node_index)
        
        if len(parent.keys) < self.min_keys and parent != self.root:
            self._rebalance_internal(parent)
        elif len(parent.keys) == 0 and parent == self.root:
            if parent.children:
                self.root = parent.children[0]
                self.root.parent = None
    
    def range_search(self, min_value, max_value):
        """Busca registros cuyos valores de atributo estén en el rango [min_value, max_value]."""
        result = []
        
        leaf = self._find_leaf_by_value(min_value)
        current_leaf = leaf
        
        while current_leaf is not None:
            for record_num in current_leaf.keys:
                record_value = self.get_attribute_from_record_num(record_num)
                if record_value is not None:
                    try:
                        if min_value <= record_value <= max_value:
                            result.append(record_num)
                        elif record_value > max_value:
                            return result
                    except TypeError:
                        min_str = str(min_value)
                        max_str = str(max_value)
                        record_str = str(record_value)
                        if min_str <= record_str <= max_str:
                            result.append(record_num)
                        elif record_str > max_str:
                            return result
            current_leaf = current_leaf.next
        
        return result
    
    def get_all_data(self):
        """Retorna todos los números de registro del árbol ordenados por valor de atributo"""
        result = []
        leaf = self._get_first_leaf()
        
        while leaf is not None:
            result.extend(leaf.keys)
            leaf = leaf.next
        
        return result
    
    def _get_first_leaf(self):
        """Encuentra el primer nodo hoja (más a la izquierda)"""
        node = self.root
        while not node.is_leaf:
            node = node.children[0]
        return node

    def rebuild_index(self):
        """Reconstruye el índice desde cero leyendo todos los registros de la tabla"""
        print(f"Reconstruyendo índice para {self.table_name} atributo {self.index_attr}...")
        
        # Reinicializar árbol
        self.root = BPlusTreeNode(is_leaf=True)
        
        # Leer tabla y reconstruir índice
        tabla_filename = f"tablas/{self.table_name}.bin"
        tabla_header_format = "<i"
        tabla_header_size = struct.calcsize(tabla_header_format)
        
        try:
            with open(tabla_filename, 'rb') as f:
                # Leer número de registros
                header_data = f.read(tabla_header_size)
                if header_data:
                    num_records = struct.unpack(tabla_header_format, header_data)[0]
                    
                    # Insertar cada registro en el índice
                    for record_num in range(1, num_records + 1):
                        # Leer registro para verificar que existe
                        position = tabla_header_size + (record_num - 1) * self.record_size
                        f.seek(position)
                        record_data = f.read(self.record_size)
                        
                        if record_data and len(record_data) == self.record_size:
                            # No llamar a insert_record para evitar guardado automático
                            leaf = self._find_leaf_for_record(record_num)
                            if record_num not in leaf.keys:
                                self._insert_record_in_leaf(leaf, record_num)
                                if len(leaf.keys) > self.max_keys:
                                    self._split_leaf(leaf)
                    
                    # Guardar una sola vez al final
                    self.save_index()
                    print(f"Índice reconstruido con {num_records} registros")
                    
        except FileNotFoundError:
            print(f"Archivo de tabla {tabla_filename} no encontrado")
        except Exception as e:
            print(f"Error al reconstruir índice: {e}")
