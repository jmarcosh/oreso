from src.inventory.common import read_files_and_backup_inventory, allocate_stock, \
    assign_warehouse_codes_from_column_and_update_inventory, create_po_path_for_savings
from src.inventory.manual_adjustments import run_manual_adjustments
from src.inventory.process_purchase_orders import run_process_purchase_orders
from src.inventory.varnames import ColNames as C


def run_po_parser(delivery_date, rfid_series=None):
    po, inventory, config, customer, matching_column = read_files_and_backup_inventory()
    po[C.DELIVERED] = allocate_stock(po, inventory, matching_column)
    po[C.DELIVERY_DATE] = delivery_date
    po = assign_warehouse_codes_from_column_and_update_inventory(po, inventory, matching_column)
    po_num = po.loc[0, C.PO_NUM]
    po_path = create_po_path_for_savings(customer, delivery_date, po_num)
    if customer in ['liverpool', 'suburbia']:
        po.to_csv(f'../../files/inventory/raw_pos/{customer}/{po_num}.csv', index=False)
        run_process_purchase_orders(po, config, customer, delivery_date, po_path, rfid_series)
    else:
        po[C.STORE_ID] = 0
        run_manual_adjustments(po, config, customer, delivery_date, po_path)

if __name__ == '__main__':
    RFID_SERIES = [['C52767864', 'C52768000'],
                   ['C56916036', 'C56917000']]
    DELIVERY_DATE = "8/16/2025"
    run_po_parser(DELIVERY_DATE, RFID_SERIES)
