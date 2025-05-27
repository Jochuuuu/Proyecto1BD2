const API_BASE_URL = 'http://localhost:8000';

let currentTable = null;
let queryHistory = [];

document.addEventListener('DOMContentLoaded', function() {
    initializeApp();
});

async function initializeApp() {
    
    await checkConnection();
    
    await loadTables();
    
    setupEventListeners();
    
}

async function checkConnection() {
    const statusElement = document.getElementById('connectionStatus');
    
    try {
        const response = await fetch(`${API_BASE_URL}/`);
        const data = await response.json();
        
        if (response.ok) {
            statusElement.className = 'connection-status connected';
            statusElement.innerHTML = '<i class="fas fa-circle"></i><span>Conectado</span>';
            showToast('Conectado al servidor', 'success');
        } else {
            throw new Error('Servidor no disponible');
        }
    } catch (error) {
        statusElement.className = 'connection-status disconnected';
        statusElement.innerHTML = '<i class="fas fa-circle"></i><span>Desconectado</span>';
        showToast('Error de conexión con el servidor', 'error');
        console.error('Error de conexión:', error);
    }
}

async function loadTables() {
    const tablesList = document.getElementById('tablesList');
    
    try {
        tablesList.innerHTML = '<div class="loading"><i class="fas fa-spinner fa-spin"></i> Cargando tablas...</div>';
        
        const response = await fetch(`${API_BASE_URL}/tables`);
        const data = await response.json();
        
        if (response.ok && data.success) {
            displayTables(data.tables);
        } else {
            throw new Error(data.error || 'Error al cargar tablas');
        }
    } catch (error) {
        tablesList.innerHTML = `
            <div class="no-results">
                <i class="fas fa-exclamation-triangle"></i>
                <p>Error al cargar tablas</p>
            </div>
        `;
        console.error('Error cargando tablas:', error);
    }
}

function displayTables(tables) {
    const tablesList = document.getElementById('tablesList');
    
    if (Object.keys(tables).length === 0) {
        tablesList.innerHTML = `
            <div class="no-results">
                <i class="fas fa-table"></i>
                <p>No hay tablas creadas</p>
            </div>
        `;
        return;
    }
    
    let html = '';
    for (const [tableName, tableInfo] of Object.entries(tables)) {
        const recordCount = tableInfo.record_count || 0;
        const primaryKey = tableInfo.primary_key || '';
        
        html += `
            <div class="table-item" onclick="selectTable('${tableName}')">
                <i class="fas fa-table table-icon"></i>
                <div class="table-info">
                    <div class="table-name">${tableName}</div>
                    <div class="table-records">${recordCount} registros</div>
                </div>
            </div>
        `;
    }
    
    tablesList.innerHTML = html;
}

function selectTable(tableName) {
    document.querySelectorAll('.table-item').forEach(item => {
        item.classList.remove('active');
    });
    
    event.target.closest('.table-item').classList.add('active');
    currentTable = tableName;
    
    const query = `SELECT * FROM ${tableName};`;
    document.getElementById('sqlQuery').value = query;
    
    showToast(`Tabla ${tableName} seleccionada`, 'success');
}

async function executeQuery() {
    const queryInput = document.getElementById('sqlQuery');
    const sql = queryInput.value.trim();
    
    if (!sql) {
        showToast('Ingresa una consulta SQL', 'warning');
        return;
    }
    
    showLoading(true);
    
    try {
        const startTime = Date.now();
        
        const response = await fetch(`${API_BASE_URL}/sql`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ sql: sql })
        });
        
        const data = await response.json();
        const executionTime = (Date.now() - startTime) / 1000;
        
        if (response.ok && data.success) {
            displayResults(data, executionTime);
            addToHistory(sql, data);
            showToast('Consulta ejecutada exitosamente', 'success');
            
            if (sql.toUpperCase().includes('CREATE TABLE') || sql.toUpperCase().includes('DROP TABLE')) {
                await loadTables();
            }
        } else {
            throw new Error(data.detail || 'Error en la consulta');
        }
        
    } catch (error) {
        displayError(error.message);
        showToast(`Error: ${error.message}`, 'error');
        console.error('Error ejecutando consulta:', error);
    } finally {
        showLoading(false);
    }
}

function displayResults(data, executionTime) {
    const resultsInfo = document.getElementById('resultsInfo');
    const resultsTable = document.getElementById('resultsTable');
    const noResults = document.getElementById('noResults');
    const explainContent = document.getElementById('explainContent');
    
    let totalRecords = 0;
    let hasSelectResults = false;
    
    for (const result of data.results) {
        if (result.operation === 'SELECT' && result.records) {
            hasSelectResults = true;
            totalRecords = result.records_found || result.records.length;
            displayTable(result.records);
            break;
        } else if (result.operation === 'INSERT') {
            totalRecords = result.records_inserted || 0;
        } else if (result.operation === 'DELETE') {
            totalRecords = result.records_deleted || 0;
        } else if (result.operation === 'IMPORT_CSV') {
            totalRecords = result.records_imported || 0;
        }
    }
    
    // Actualizar info header
    resultsInfo.innerHTML = `
        <span class="record-count">${totalRecords} records</span>
        <span class="execution-time">${executionTime.toFixed(3)} sec</span>
    `;
    
    if (hasSelectResults) {
        noResults.style.display = 'none';
        resultsTable.style.display = 'table';
    } else {
        noResults.style.display = 'flex';
        resultsTable.style.display = 'none';
        
        const operation = data.results[0]?.operation || 'UNKNOWN';
        const message = data.results[0]?.message || 'Operación completada';
        
        noResults.innerHTML = `
            <i class="fas fa-check-circle" style="color: var(--success-color);"></i>
            <p><strong>${operation}</strong></p>
            <p>${message}</p>
        `;
    }
    
    // Actualizar explain tab
    explainContent.textContent = JSON.stringify(data, null, 2);
}

function displayTable(records) {
    const tableHeaders = document.getElementById('tableHeaders');
    const tableBody = document.getElementById('tableBody');
    
    if (!records || records.length === 0) {
        tableHeaders.innerHTML = '';
        tableBody.innerHTML = '<tr><td colspan="100%">No hay datos para mostrar</td></tr>';
        return;
    }
    
    // Crear headers
    const firstRecord = records[0];
    const headers = Object.keys(firstRecord);
    
    tableHeaders.innerHTML = headers.map(header => 
        `<th>${header}</th>`
    ).join('');
    
    // Crear filas
     tableBody.innerHTML = records.map((record, index) => {
        const cells = headers.map(header => {
            let value = record[header];
            
            if (value === null || value === undefined) {
                value = '<span class="null-value">NULL</span>';
            } else if (typeof value === 'object') {
                    if (value.type === 'POINT') {
                    value = `<span class="point-value" title="${value.string_representation}">POINT(${value.x}, ${value.y})</span>`;
                } else {
                    value = `<span class="object-value">${JSON.stringify(value)}</span>`;
                }
            } else if (typeof value === 'string' && value.length > 50) {
                value = `<span title="${value}">${value.substring(0, 47)}...</span>`;
            }
            
            return `<td>${value}</td>`;
        }).join('');
        
        return `<tr class="data-row" data-row-index="${index}">${cells}</tr>`;
    }).join('');
}

// Mostrar error
function displayError(errorMessage) {
    const noResults = document.getElementById('noResults');
    const resultsTable = document.getElementById('resultsTable');
    const resultsInfo = document.getElementById('resultsInfo');
    
    resultsInfo.innerHTML = `
        <span class="record-count">0 records</span>
        <span class="execution-time">Error</span>
    `;
    
    noResults.style.display = 'flex';
    resultsTable.style.display = 'none';
    
    noResults.innerHTML = `
        <i class="fas fa-exclamation-triangle" style="color: var(--error-color);"></i>
        <p><strong>Error en la consulta</strong></p>
        <p>${errorMessage}</p>
    `;
}

// Manejar tabs
function showTab(tabName) {
    // Remover active de todos los tabs
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
    
    // Activar tab seleccionado
    event.target.classList.add('active');
    document.getElementById(tabName + 'Tab').classList.add('active');
}

// Limpiar consulta
function clearQuery() {
    document.getElementById('sqlQuery').value = '';
    document.getElementById('sqlQuery').focus();
}

function insertSuggestion(suggestion) {
    const queryInput = document.getElementById('sqlQuery');
    const currentValue = queryInput.value;
    const cursorPos = queryInput.selectionStart;
    
    const newValue = currentValue.substring(0, cursorPos) + suggestion + currentValue.substring(cursorPos);
    queryInput.value = newValue;
    
    queryInput.focus();
    queryInput.setSelectionRange(cursorPos + suggestion.length, cursorPos + suggestion.length);
}

function showLoading(show) {
    const overlay = document.getElementById('loadingOverlay');
    if (show) {
        overlay.classList.add('show');
    } else {
        overlay.classList.remove('show');
    }
}

function showToast(message, type = 'success') {
    const toast = document.getElementById('toast');
    const toastMessage = toast.querySelector('.toast-message');
    const toastIcon = toast.querySelector('.toast-icon');
    
    toastMessage.textContent = message;
    
    toast.className = `toast ${type}`;
    
    switch (type) {
        case 'success':
            toastIcon.className = 'toast-icon fas fa-check-circle';
            break;
        case 'error':
            toastIcon.className = 'toast-icon fas fa-exclamation-circle';
            break;
        case 'warning':
            toastIcon.className = 'toast-icon fas fa-exclamation-triangle';
            break;
        default:
            toastIcon.className = 'toast-icon fas fa-info-circle';
    }
    
    toast.classList.add('show');
    
    setTimeout(() => {
        toast.classList.remove('show');
    }, 3000);
}

function addToHistory(sql, result) {
    queryHistory.unshift({
        sql: sql,
        timestamp: new Date(),
        result: result
    });
    
    if (queryHistory.length > 50) {
        queryHistory = queryHistory.slice(0, 50);
    }
}

function setupEventListeners() {
    document.getElementById('sqlQuery').addEventListener('keydown', function(e) {
        if (e.ctrlKey && e.key === 'Enter') {
            e.preventDefault();
            executeQuery();
        }
    });
    
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape') {
            showLoading(false);
        }
    });
    
    document.getElementById('toast').addEventListener('click', function() {
        this.classList.remove('show');
    });
}

function insertCreateTable() {
    const template = `CREATE TABLE NombreTabla (
    id INT PRIMARY KEY INDEX BTree,
    nombre VARCHAR[100] INDEX BTree,
    descripcion VARCHAR[255],
    fecha_creacion VARCHAR[20]
);`;
    document.getElementById('sqlQuery').value = template;
}

function insertSelect() {
    if (currentTable) {
        document.getElementById('sqlQuery').value = `SELECT * FROM ${currentTable};`;
    } else {
        document.getElementById('sqlQuery').value = 'SELECT * FROM NombreTabla;';
    }
}

function insertInsert() {
    if (currentTable) {
        document.getElementById('sqlQuery').value = `INSERT INTO ${currentTable} VALUES ();`;
    } else {
        document.getElementById('sqlQuery').value = 'INSERT INTO NombreTabla VALUES ();';
    }
}

function insertUpdate() {
    if (currentTable) {
        document.getElementById('sqlQuery').value = `UPDATE ${currentTable} SET columna = valor WHERE condicion;`;
    } else {
        document.getElementById('sqlQuery').value = 'UPDATE NombreTabla SET columna = valor WHERE condicion;';
    }
}

function insertDelete() {
    if (currentTable) {
        document.getElementById('sqlQuery').value = `DELETE FROM ${currentTable} WHERE condicion;`;
    } else {
        document.getElementById('sqlQuery').value = 'DELETE FROM NombreTabla WHERE condicion;';
    }
}

window.executeQuery = executeQuery;
window.clearQuery = clearQuery;
window.insertSuggestion = insertSuggestion;
window.showTab = showTab;
window.selectTable = selectTable;
window.loadTables = loadTables;

if (window.location.hostname === 'localhost') {
    window.debugAPI = {
        checkConnection,
        loadTables,
        executeQuery,
        showToast,
        queryHistory: () => queryHistory
    };
    console.log('🔧 Debug helpers disponibles en window.debugAPI');
}