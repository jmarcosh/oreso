import json

from src.inventory.varnames import ColNames as C


data = {
    "liverpool_rename": {
        'Orden Compra': C.PO_NUM,
        'Tip Eti': C.LABEL_TYPE,
        'Sku': C.SKU,
        'Ean/Upc': C.CUSTOMER_UPC,
        'Modelo': C.CUSTOMER_STYLE,
        'depto': C.SECTION,
        'Costo': C.CUSTOMER_COST,
        'Precio Normal': C.CUSTOMER_PRICE,
        'Tienda': C.STORE_ID,
        'Nombre tienda': C.STORE_NAME,
        'Cantidad': C.ORDERED
    },

    "suburbia_rename": {
        'Orden Compra': C.PO_NUM,
        'Tipo Eti.': C.LABEL_TYPE,
        'Sku': C.SKU,
        'Ean/Upc': C.CUSTOMER_UPC,
        'Modelo': C.CUSTOMER_STYLE,
        'Num. Depto': C.SECTION,
        'Costo Uni': C.CUSTOMER_COST,
        'Precio Regular': C.CUSTOMER_PRICE,
        'Tda Distr.': C.STORE_ID,
        'Nombre tienda': C.STORE_NAME,
        'Cantidad': C.ORDERED
    },

    "interno_rename": {
        C.PO_NUM: C.PO_NUM,
        C.WAREHOUSE_CODE: C.WAREHOUSE_CODE,
        'CANTIDAD': C.ORDERED,
    },

    "br_columns": [
        C.COLLECTED, C.DELIVERY_DATE, C.INVOICE_DATE, C.KEY, C.BUS_KEY,
        C.CUSTOMER, C.PO_NUM, C.INVOICE_NUM, C.SHIPPED, C.WAREHOUSE_CODE,
        C.STYLE, C.DESCRIPTION, C.UPC, C.SKU, C.ORDERED, C.DELIVERED,
        C.INVOICED, C.WHOLESALE_PRICE, C.SUBTOTAL, C.DISCOUNT,
        C.SUBTOTAL_NET, C.VAT, C.GROUP
    ],

    "ts_rename": {
        C.PO_NUM: '# OC',
        C.WAREHOUSE_CODE: 'Código Tecs',
        C.STYLE: 'Grupo',
        C.DESCRIPTION: 'Descripción',
        C.SKU: 'Sku',
        C.STORE_ID: '# Sucursal',
        C.STORE_NAME: 'Nombre sucursal',
        C.DELIVERED: 'Cantidad',
        'BOX_STORE_NUM': 'Caja inicial',
        C.BOX_ID: 'Contenedor',
        C.BOX_TYPE: 'Tipo Caja',
    },

    "ts_columns_csv": [
        'Código Tecs', 'Grupo', 'Descripción', 'Sku',
        '# Sucursal', 'Contenedor', 'Cantidad'
    ],

    "ts_columns_txt": [
        "Tipo", "FECHA", "Cliente final", "# factura", "# OC", "Código Tecs", "Grupo", "Descripción",
        "Sku", "# Sucursal", "Nombre sucursal", "Unidad", "Cantidad", "Factor",
        "Caja inicial", "Caja final", "FECHA DE CITA", "# Cita", "OBSERVACIÓN"
    ],

    "asn_rename": {
        C.PO_NUM: 'PEDIDO',
        C.STORE_ID: 'CENTRO',
        C.BOX_ID: 'HU DEL CONTENEDOR',
        C.DELIVERED: 'PIEZAS CITADAS POR HU',
        'LENGTH': 'LARGO (CM)',
        'WIDTH': 'ANCHO (CM)',
        'HEIGHT': 'ALTO (CM)'
    },

    "asn_columns": [
        "PEDIDO", C.CUSTOMER_UPC, C.SKU, "CENTRO/ALMACEN DESTINO", "HU DEL CONTENEDOR", "PIEZAS CITADAS POR HU",
        "FECHA FIN DE CADUCIDAD DEL SKU", "ALTO (CM)", "LARGO (CM)", "ANCHO (CM)", "MERCANCÍA SIN ETIQUETA",
        "SELLO 1 DEL HU", "SELLO 2 DEL HU", "TRANSPORTE"
    ],

    "cartons": [
        {"name": "RM-51", "capacity": 23760, "cost": 24.46, "dimensions": (43, 33, 22)},
        {"name": "RM-32", "capacity": 17820, "cost": 17.25, "dimensions": (29, 29, 22)},
        {"name": "RM-31", "capacity": 7920, "cost": 11.5, "dimensions": (30, 21, 14)},
    ],

    "checklist_indexes": [
        C.RD, C.MOVEX_PO, C.PO_NUM, C.WAREHOUSE_CODE, C.STYLE, C.DESCRIPTION,
        C.UPC, C.SKU, C.BRAND, C.GROUP
    ],

    "checklist_values": {
        C.INVENTORY: 'mean',
        C.ORDERED: 'sum',
        C.DELIVERED: 'sum',
        C.WHOLESALE_PRICE: 'mean',
        C.CUSTOMER_COST: 'mean',
    },

    "store_indexes": [C.STORE_ID, C.BOX_ID, C.BOX_TYPE],

    "dn_structure": [
        ("NOTA DE REMISION", 2000),
        ("Cliente:", "Distribuidora Liverpool SA de CV"),
        ("RFC:", "DLI931201MI9"),
        ("Dirección:", "Mario Pani 200 Col. Santa Fé Del. Cuajimalpa de Morelos CP 05109"),
        ("Proveedor:", "Grupo Oreso SA de CV"),
        ("RFC:", "GOR120208K23"),
        ("Dirección:", "Monte Elbruz N. 124 piso 2 desp 212 Col. Palmitas C.P.  11560 Del. Miguel Hidalgo"),
        ("Número de proveedor:", "134494"),
        ("Orden de compra:", ""),
        ("Departamento", ""),
        ("Fecha orden de compra:", "")
    ],

    "dn_columns": [
        C.DELIVERED, C.STYLE, C.DESCRIPTION, C.UPC, C.SKU, C.CUSTOMER_COST
    ]
}


with open("/home/jmarcosh/Projects/oreso/files/inventory/config_vars.json", "w") as f:
    json.dump(data, f, indent=2)