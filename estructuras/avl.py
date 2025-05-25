import struct
import os
import re

class AVLFile:
    def __init__(self, record_format="<i50sdii", index_attr=2, table_name="Productos", is_key=False):
        self.record_format = record_format
        self.index_attr = index_attr  # El atributo a indexar (2 = nombre)
        self.table_name = table_name
        self.is_key = is_key  # Indica si el atributo es una clave (no permite duplicados)
        
        # Analizar el formato para determinar los tipos de campo
        self.field_types = self._parse_format(record_format)
        
        # Configurar el formato del registro
        self.record_size = struct.calcsize(self.record_format)
        
        # Asegurar que los directorios existan
        os.makedirs("indices", exist_ok=True)
        
        # Nombre del archivo de índice AVL
        self.filename = f"indices/{table_name}_{index_attr}_avl.dat"
        
        # Formato de cabecera: root_index, header_index
        self.header_format = 'i i'
        # Formato de nodo: clave, left, right, height, next
        self.struct_format = 'i i i i i'  
        self.record_node_size = struct.calcsize(self.struct_format)
        self.header_size = struct.calcsize(self.header_format)
        
        # Crear o inicializar el archivo si no existe
        if not os.path.exists(self.filename) or os.path.getsize(self.filename) == 0:
            with open(self.filename, 'wb') as f:
                # Inicializar cabecera con root=0, header=-1 (lista vacía)
                f.write(struct.pack(self.header_format, 0, -1))

    def _parse_format(self, format_str):
        """Analiza el formato del registro para determinar los tipos de cada campo."""
        # Patrón para extraer los tipos de campos del formato
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

    def get_attribute_from_record_num(self, record_num):
        """
        Obtiene el valor del atributo indexado desde un número de registro en el archivo de tablas.
        Args:
            record_num (int): Número de registro (empezando desde 1)
        Returns:
            El valor del atributo indexado, o None si hay error
        """
        tabla_filename = f"tablas/{self.table_name}.bin"
        tabla_header_format = "<i"  # 4 bytes para la cabecera (int)
        tabla_header_size = struct.calcsize(tabla_header_format)
        
        try:
            with open(tabla_filename, 'rb') as f:
                # Calcular la posición del registro
                position = tabla_header_size + (record_num - 1) * self.record_size
                f.seek(position)
                
                # Leer el registro completo
                record_data = f.read(self.record_size)
                
                if not record_data or len(record_data) < self.record_size:
                    print(f"Error: No se pudo leer el registro {record_num}")
                    return None
                
                # Desempaquetar los datos usando el formato
                unpacked_data = list(struct.unpack(self.record_format, record_data))
                
                # Obtener el valor del atributo indexado (index_attr empezando desde 1)
                if self.index_attr < 1 or self.index_attr > len(unpacked_data):
                    print(f"Error: index_attr {self.index_attr} fuera de rango")
                    return None
                    
                indexed_value = unpacked_data[self.index_attr - 1]
                
                # Convertir según el tipo de campo
                field_type = self.field_types[self.index_attr - 1] if self.index_attr - 1 < len(self.field_types) else 'unknown'
                if field_type == 'string':
                    indexed_value = indexed_value.decode('utf-8').rstrip('\0')
                
                return indexed_value
                
        except FileNotFoundError:
            print(f"Error: El archivo {tabla_filename} no existe.")
            return None
        except Exception as e:
            print(f"Error al leer el atributo del registro {record_num}: {e}")
            return None

    def _compare_keys(self, record_num1, record_num2):
        """
        Compara dos números de registro basándose en el valor de sus atributos indexados.
        Retorna: -1 si attr1 < attr2, 0 si attr1 == attr2, 1 si attr1 > attr2
        """
        valor1 = self.get_attribute_from_record_num(record_num1)
        valor2 = self.get_attribute_from_record_num(record_num2)
        
        if valor1 is None or valor2 is None:
            return 0  # En caso de error, considerarlos iguales
        
        if valor1 < valor2:
            return -1
        elif valor1 > valor2:
            return 1
        else:
            return 0

    def _read_header(self):
        with open(self.filename, 'rb') as f:
            data = f.read(self.header_size)
            root_index, header_index = struct.unpack(self.header_format, data)
            return {'root': root_index, 'header': header_index}

    def _write_header(self, root_index, header_index):
        with open(self.filename, 'rb+') as f:
            f.seek(0)
            data = struct.pack(self.header_format, root_index, header_index)
            f.write(data)

    def _get_height(self, index):
        if index == 0:
            return 0
        return self._read_node(index)['height']

    def _update_height(self, index):
        if index == 0:
            return
        node = self._read_node(index)
        left_height = self._get_height(node['left'])
        right_height = self._get_height(node['right'])
        node['height'] = 1 + max(left_height, right_height)
        self._write_node(index, node)

    def _read_node(self, index):
        with open(self.filename, 'rb') as f:
            # Ajustar posición debido a la cabecera
            f.seek(self.header_size + (index - 1) * self.record_node_size)
            data = f.read(self.record_node_size)
            clave, left, right, height, next_val = struct.unpack(self.struct_format, data)
            return {'clave': clave, 'left': left, 'right': right, 'height': height, 'next': next_val}

    def _write_node(self, index, node):
        with open(self.filename, 'rb+') as f:
            # Ajustar posición debido a la cabecera
            f.seek(self.header_size + (index - 1) * self.record_node_size)
            data = struct.pack(self.struct_format, node['clave'], node['left'], node['right'], node['height'], node['next'])
            f.write(data)

    def _create_node(self, clave, left=0, right=0, height=1):
        # Verificar si hay nodos liberados para reutilizar
        header = self._read_header()
        free_index = header['header']
        
        if free_index != -1:
            # Hay nodos libres, reutilizar el primero
            free_node = self._read_node(free_index)
            next_free = free_node['next']  # Siguiente nodo libre
            
            # Actualizar la cabecera para que apunte al siguiente nodo libre
            self._write_header(header['root'], next_free)
            
            # Reutilizar el espacio
            index = free_index
            node = {'clave': clave, 'left': left, 'right': right, 'height': height, 'next': -2}  # -2 para nodos en uso
            self._write_node(index, node)
            print(f"Reutilizando nodo libre {index}")
            return index
        else:
            # No hay nodos libres, crear uno nuevo al final del archivo
            node = {'clave': clave, 'left': left, 'right': right, 'height': height, 'next': -2}  # -2 para nodos en uso
            with open(self.filename, 'ab') as f:
                # El índice ahora depende del tamaño del archivo y la cabecera
                index = (f.tell() - self.header_size) // self.record_node_size + 1
                data = struct.pack(self.struct_format, clave, left, right, height, -2)
                f.write(data)
                return index

    def _add_to_free_list(self, index):
        # Añadir un nodo a la free list
        header = self._read_header()
        current_free_list = header['header']
        
        # El nodo liberado apunta al inicio actual de la free list
        freed_node = {'clave': 0, 'left': 0, 'right': 0, 'height': 0, 'next': current_free_list}
        self._write_node(index, freed_node)
        
        # La cabecera ahora apunta a este nodo como inicio de la free list
        self._write_header(header['root'], index)
        
        print(f"Nodo {index} añadido a la free list. Free list ahora comienza en {index}")

    def _balance_factor(self, index):
        if index == 0:
            return 0
        node = self._read_node(index)
        return self._get_height(node['left']) - self._get_height(node['right'])

    def _rotate_right(self, y_index):
        y = self._read_node(y_index)
        x_index = y['left']
        if x_index == 0:
            return y_index
        x = self._read_node(x_index)
        T2_index = x['right']

        # Rotación
        x['right'] = y_index
        y['left'] = T2_index

        self._write_node(y_index, y)
        self._write_node(x_index, x)

        # Actualizar alturas después de la rotación
        self._update_height(y_index)
        self._update_height(x_index)

        return x_index

    def _rotate_left(self, x_index):
        x = self._read_node(x_index)
        y_index = x['right']
        if y_index == 0:
            return x_index
        y = self._read_node(y_index)
        T2_index = y['left']

        # Rotación
        y['left'] = x_index
        x['right'] = T2_index

        self._write_node(x_index, x)
        self._write_node(y_index, y)

        # Actualizar alturas después de la rotación
        self._update_height(x_index)
        self._update_height(y_index)

        return y_index

    def _rebalance(self, index):
        if index == 0:
            return 0
            
        self._update_height(index)
        balance = self._balance_factor(index)
        node = self._read_node(index)

        # Caso izquierda-izquierda
        if balance > 1:
            left_index = node['left']
            if left_index != 0 and self._balance_factor(left_index) >= 0:
                return self._rotate_right(index)
            # Caso izquierda-derecha
            elif left_index != 0:
                node['left'] = self._rotate_left(left_index)
                self._write_node(index, node)
                return self._rotate_right(index)

        # Caso derecha-derecha
        if balance < -1:
            right_index = node['right']
            if right_index != 0 and self._balance_factor(right_index) <= 0:
                return self._rotate_left(index)
            # Caso derecha-izquierda
            elif right_index != 0:
                node['right'] = self._rotate_right(right_index)
                self._write_node(index, node)
                return self._rotate_left(index)

        return index

    def insert_record(self, clave):
        # Leer la cabecera para obtener el root_index actual
        header = self._read_header()
        root_index = header['root']
        
        # Si es un árbol de claves (sin duplicados) y la clave ya existe, no insertarla
        if self.is_key and root_index != 0:
            valor_clave = self.get_attribute_from_record_num(clave)
            if valor_clave is not None:
                results = self.search(valor_clave)
                if results:
                    print(f"Clave {clave} (valor: {valor_clave}) ya existe y no se permiten duplicados.")
                    return root_index
        
        if root_index == 0:
            # Primer nodo en el árbol
            root_index = self._create_node(clave)
        else:
            root_index = self._insert_rec(clave, root_index)
            
        # Actualizar el root_index en la cabecera
        header = self._read_header()  # Volver a leer la cabecera para obtener el valor actualizado de header
        self._write_header(root_index, header['header'])
        return root_index

    def _insert_rec(self, clave, root_index):
        if root_index == 0:
            return self._create_node(clave)
            
        root_node = self._read_node(root_index)
        
        # Usar comparación por valor de atributo
        comparison = self._compare_keys(clave, root_node['clave'])
        
        if comparison < 0:  # clave < root_node['clave'] (por valor de atributo)
            root_node['left'] = self._insert_rec(clave, root_node['left'])
            self._write_node(root_index, root_node)
        elif comparison > 0:  # clave > root_node['clave'] (por valor de atributo)
            root_node['right'] = self._insert_rec(clave, root_node['right'])
            self._write_node(root_index, root_node)
        else:  # comparison == 0, valores iguales
            if self.is_key:
                # Si es árbol de claves únicas, reemplazar el nodo actual (o no hacer nada)
                return root_index
            else:
                # Si permite duplicados, insertar a la derecha
                root_node['right'] = self._insert_rec(clave, root_node['right'])
                self._write_node(root_index, root_node)
            
        # Actualizar altura y rebalancear
        return self._rebalance(root_index)

    def _min_value_node(self, node_index):
        current_index = node_index
        current_node = self._read_node(current_index)
        
        while current_node['left'] != 0:
            current_index = current_node['left']
            current_node = self._read_node(current_index)
            
        return current_index

    def search(self, target_value):
        """
        Busca registros que tengan el valor específico en el atributo indexado.
        target_value: valor del atributo a buscar (ej: 45.75 para precio)
        """
        header = self._read_header()
        root_index = header['root']
        
        results = []
        self._search_rec(root_index, target_value, results)
        record_numbers = [node['clave'] for node in results]
        return record_numbers

    def _search_rec(self, root_index, target_value, results):
        """
        Busca por valor de atributo, no por número de registro.
        target_value: el valor del atributo a buscar (ej: precio 45.75)
        """
        if root_index == 0:
            return
        
        root_node = self._read_node(root_index)
        current_value = self.get_attribute_from_record_num(root_node['clave'])
        
        if current_value is None:
            return
        
        if target_value < current_value:
            self._search_rec(root_node['left'], target_value, results)
        elif target_value > current_value:
            self._search_rec(root_node['right'], target_value, results)
        else:
            # Encontramos un nodo con el valor buscado
            results.append(root_node)
            
            # Si permite duplicados, buscar en AMBOS subárboles
            # porque los duplicados pueden estar tanto a la izquierda como a la derecha
            if not self.is_key:
                self._search_rec(root_node['left'], target_value, results)
                self._search_rec(root_node['right'], target_value, results)

    def range_search(self, min_value, max_value):
        """
        Busca registros cuyos valores de atributo estén en el rango [min_value, max_value].
        """
        header = self._read_header()
        root_index = header['root']
        
        results = []
        self._range_search_rec(root_index, min_value, max_value, results)
        record_numbers = [node['clave'] for node in results]
        return record_numbers

    def _range_search_rec(self, root_index, min_value, max_value, results):
        """
        Búsqueda por rango basada en valores de atributos.
        """
        if root_index == 0:
            return

        root_node = self._read_node(root_index)
        current_value = self.get_attribute_from_record_num(root_node['clave'])
        
        if current_value is None:
            return

        # Si el valor mínimo del rango es menor que el nodo actual, buscar en el subárbol izquierdo
        if min_value < current_value:
            self._range_search_rec(root_node['left'], min_value, max_value, results)

        # Si el nodo actual está en el rango, añadirlo a los resultados
        if min_value <= current_value <= max_value:
            results.append(root_node)

        # Si el valor máximo del rango es mayor o igual al nodo actual, buscar en el subárbol derecho
        if current_value <= max_value:
            self._range_search_rec(root_node['right'], min_value, max_value, results)

    def delete_record(self, record_num):
        """
        Elimina un registro específico del índice AVL por su número de registro.
        VERSIÓN DEFINITIVA que funciona con cualquier tipo de atributo.
        
        Args:
            record_num (int): Número de registro a eliminar del índice
            
        Returns:
            int: El número de registro eliminado si tuvo éxito, None en caso contrario
        """
        header = self._read_header()
        root_index = header['root']
        
        if root_index == 0:
            return None
        
        # Obtener el valor del atributo para este registro
        target_value = self.get_attribute_from_record_num(record_num)
        if target_value is None:
            return None
        
        # Verificar que el registro existe en el árbol usando búsqueda completa
        if not self._search_record_in_subtree(root_index, record_num):
            return None
        
        # Eliminar el registro del árbol
        new_root_index = self._delete_specific_record_rec(root_index, record_num, target_value)
        
        # Actualizar el root_index en la cabecera
        header = self._read_header()
        self._write_header(new_root_index, header['header'])
        
        return record_num

    def _search_record_in_subtree(self, root_index, target_record_num):
        """
        Busca un número de registro específico en todo el subárbol.
        Funciona con cualquier tipo de atributo y estructura de árbol.
        
        Args:
            root_index (int): Índice del nodo raíz del subárbol
            target_record_num (int): Número de registro a buscar
            
        Returns:
            bool: True si el registro existe en este subárbol
        """
        if root_index == 0:
            return False
        
        root_node = self._read_node(root_index)
        current_record_num = root_node['clave']
        
        # Si encontramos el registro
        if current_record_num == target_record_num:
            return True
        
        # Buscar en ambos subárboles (porque no sabemos dónde puede estar)
        found_left = self._search_record_in_subtree(root_node['left'], target_record_num)
        if found_left:
            return True
            
        found_right = self._search_record_in_subtree(root_node['right'], target_record_num)
        return found_right

    def _delete_specific_record_rec(self, root_index, target_record_num, target_value):
        """
        Elimina recursivamente un registro específico buscando por valor Y número de registro.
        Funciona correctamente con cualquier tipo de atributo (int, string, float, etc.).
        
        Args:
            root_index (int): Índice del nodo raíz actual
            target_record_num (int): Número de registro a eliminar
            target_value: Valor del atributo del registro
            
        Returns:
            int: Nuevo índice de la raíz después de la eliminación
        """
        if root_index == 0:
            return 0
        
        root_node = self._read_node(root_index)
        current_record_num = root_node['clave']
        current_value = self.get_attribute_from_record_num(current_record_num)
        
        if current_value is None:
            return root_index
        
        # Si encontramos el registro exacto (mismo número Y mismo valor)
        if current_record_num == target_record_num and current_value == target_value:
            # Eliminar este nodo
            return self._remove_node(root_index)
        
        # Buscar en el subárbol apropiado basado en el valor del atributo
        if target_value < current_value:
            root_node['left'] = self._delete_specific_record_rec(root_node['left'], target_record_num, target_value)
            self._write_node(root_index, root_node)
        elif target_value > current_value:
            root_node['right'] = self._delete_specific_record_rec(root_node['right'], target_record_num, target_value)
            self._write_node(root_index, root_node)
        else:
            # Mismo valor de atributo, pero diferente número de registro
            # Buscar en ambos subárboles porque pueden haber duplicados
            root_node['left'] = self._delete_specific_record_rec(root_node['left'], target_record_num, target_value)
            root_node['right'] = self._delete_specific_record_rec(root_node['right'], target_record_num, target_value)
            self._write_node(root_index, root_node)
        
        # Rebalancear el árbol después de la eliminación
        return self._rebalance(root_index)

    def _remove_node(self, node_index):
        """
        Remueve un nodo específico del árbol AVL.
        Maneja todos los casos de eliminación correctamente.
        
        Args:
            node_index (int): Índice del nodo a remover
            
        Returns:
            int: Nuevo índice de la raíz después de la eliminación
        """
        node = self._read_node(node_index)
        
        # Caso 1: Nodo hoja (sin hijos)
        if node['left'] == 0 and node['right'] == 0:
            self._add_to_free_list(node_index)
            return 0
            
        # Caso 2: Nodo con solo hijo derecho
        elif node['left'] == 0:
            new_root = node['right']
            self._add_to_free_list(node_index)
            return new_root
            
        # Caso 3: Nodo con solo hijo izquierdo
        elif node['right'] == 0:
            new_root = node['left']
            self._add_to_free_list(node_index)
            return new_root
            
        # Caso 4: Nodo con dos hijos
        else:
            # Encontrar el sucesor in-order (nodo más pequeño del subárbol derecho)
            successor_index = self._min_value_node(node['right'])
            successor_node = self._read_node(successor_index)
            
            # Reemplazar el contenido del nodo actual con el del sucesor
            node['clave'] = successor_node['clave']
            self._write_node(node_index, node)
            
            # Eliminar el sucesor (que ahora está duplicado)
            successor_value = self.get_attribute_from_record_num(successor_node['clave'])
            node['right'] = self._delete_specific_record_rec(node['right'], successor_node['clave'], successor_value)
            self._write_node(node_index, node)
            
            return node_index
