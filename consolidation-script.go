package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
	"github.com/xuri/excelize/v2"
)

var f = excelize.NewFile()

func main() {

	db, err := dbconn()
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	//select the Parents id's from the parents table
	//save it in a slice and retrive it to be used in the child accounts FOR loop
	parentIDQuery :=
		`
	select id from consolidated_parent_accounts;
	
	`

	rows, err := db.QueryContext(context.Background(), parentIDQuery)
	if err != nil {
		log.Fatalf("error getting parents ID")

	}

	defer rows.Close()

	//initialize a slice to store the parent ID's
	var parentIDslice []int

	for rows.Next() {
		var parentID int
		err := rows.Scan(&parentID)
		if err != nil {
			log.Fatalf("error scanning parent ID")
		}

		parentIDslice = append(parentIDslice, parentID)
	}

	//fmt.Println(parentIDslice)

	//declare temp values to store the groupname and address
	var groupNametemp, addresstemp string

	for _, parentID := range parentIDslice {

		//loop through the parent ID's and get the child services

		getChildServices :=
			`
				with ids as (
			select
				cca.id,
				cpa.group_name,
				cca.account_id,
				cca.bill_source,
				cca.status,
				cca.group_id
			from
				consolidated_child_accounts cca
			left join
				consolidated_parent_accounts cpa
			on
				cca.group_id = cpa.id
			where
				cca.group_id = $1
		)
		select
			i.group_id,
			b.period,
			b.cycle,
			i.account_id,
			i.group_name,
			b.address,
			i.bill_source,
			b.acct_serv_id,
			b.serv_name,
			b.total,
			b.taxes,
			b.discounts
		from
			(select
				bs.period,
				bs.cycle,
				bs.accountid,
				bs.acct_serv_id,
				bs.serv_name,
				bs.total,
				bs.taxes,
				bs.discounts,
				bc.address
			from
				bill_services bs
			join
				bill_cust bc
			on
				bs.accountid = bc.accountid
		) b
		right join
			ids i
		on
			b.accountid = i.account_id
		where
			(bill_source = 'inhouse_billing' and (b.period is not null and total > 0)
			or bill_source = 'bss' and b.period is null)
		order by
			account_id desc;

			`

		rows, err = db.QueryContext(context.Background(), getChildServices, parentID)
		if err != nil {
			log.Fatalf("Getting Child services %v", err)
		}

		defer rows.Close()

		for rows.Next() {
			var groupID int
			var period string
			var cycle string
			var accountID int
			var groupName string
			var address string
			var billSource string
			var acctServID int
			var servName string
			var total float64
			var taxes float64
			var discounts float64

			err := rows.Scan(&groupID, &period, &cycle, &accountID, &groupName, &address, &billSource, &acctServID, &servName, &total, &taxes, &discounts)
			if err != nil {
				log.Fatalf("error scanning child services")
			}

			//assign the groupname and address to the temp variables
			groupNametemp = groupName
			addresstemp = address

			//fmt.Println(groupID, period, cycle, accountID, groupName, address, billSource, acctServID, servName, total, taxes, discounts)

			//insert the child services into the consolidated_bills_summary table

			insetChildServices := ` INSERT INTO public.consolidated_bills_summary (group_id,"period","cycle",account_id,group_name,address,bill_source,acct_serv_id,serv_name,total,taxes,discount,created_at)
											VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW())`

			_, err = db.ExecContext(context.Background(), insetChildServices, groupID, period, cycle, accountID, groupName, address, billSource, acctServID, servName, total, taxes, discounts)

			if err != nil {
				log.Fatalf("%v", err)
			}

		}

		//---------------------------------API SIMULATION--------------------------------------------------------------

		//columns: customerID, billingcycle,invoiceID,total,taxes,discount,invoice_date
		bss_API_call := ` select id,customer_id,billing_cycle,invoice_id,total,taxes,discount,invoice_date from bss_simulation_api where id =$1 ; `
		rows, err = db.QueryContext(context.Background(), bss_API_call, parentID)

		if err != nil {
			log.Fatalf("%v", err)
		}

		defer rows.Close()

		for rows.Next() {
			var groupID int
			var customerID int
			var billingcycle string
			var invoiceID int
			var total float64
			var taxes float64
			var discount float64
			var invoice_date string

			err := rows.Scan(&groupID, &customerID, &billingcycle, &invoiceID, &total, &taxes, &discount, &invoice_date)
			if err != nil {
				log.Fatalf("%v", err)
			}

			//insert the BSS services into the consolidated_bills_summary table

			insertBSSservices := ` INSERT INTO public.consolidated_bills_summary (group_id,"period","cycle",account_id,group_name,address,bill_source,acct_serv_id,serv_name,total,taxes,discount,created_at)
								VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW())`

			_, err = db.ExecContext(context.Background(), insertBSSservices, groupID, invoice_date, billingcycle, customerID, groupNametemp, addresstemp, "bss", invoiceID, "Postpaid", total, taxes, discount)

			if err != nil {
				log.Fatalf("%v", err)
			}

		}

		//-----------------------------------------------------------------------------------------------

	} //end of parentID loop

	println("Child services inserted successfully")

	//call the generate report function

	generateReport(db)
	BusinessAccounts()

	filePath := "consolidated_report.xlsx"
	if err := f.SaveAs(filePath); err != nil {
		log.Fatalf("Failed to save Excel file: %v", err)
	}

	fmt.Println("Report generated successfully: ", filePath)

}

func dbconn() (*sql.DB, error) {
	err := godotenv.Load()

	if err != nil {
		return nil, fmt.Errorf("error loading .env file: %v", err)
	}

	//Initialize ENV variables (port had to be converted becasue its a string)
	host := os.Getenv("DB_HOST")
	portStr := os.Getenv("DB_PORT")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("Invalid port number: %v", err)
	}
	dbname := os.Getenv("DB_DATABASE")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")

	// Set up database connection
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("pgx", psqlInfo)
	if err != nil {
		return nil, fmt.Errorf("error opening database connection: %v", err)

	}

	return db, err

}

func generateReport(db *sql.DB) {
	query := `
        SELECT group_id, account_id, group_name, address, serv_name, total, taxes, discount
        FROM consolidated_bills_summary
        ORDER BY group_id
    `

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	type Record struct {
		GroupID   int
		AccountID int
		GroupName string
		Address   string
		Services  map[string]float64
		Total     float64
		Taxes     float64
		Discount  float64
	}

	type NewAccounts struct {
		custID       string
		customerName string
		planName     string
		address      string
		description  string
		dateSubmit   string
		installDate  string
	}

	recordsMap := make(map[int]map[string]*Record) //intialize a map made up of maps to store the records
	serviceNames := make(map[string]bool)          //intialize a map to store the service names

	for rows.Next() {
		var groupID, accountID int
		var groupName, address, servName string
		var total, taxes, discount float64
		if err := rows.Scan(&groupID, &accountID, &groupName, &address, &servName, &total, &taxes, &discount); err != nil {
			log.Fatal(err)
		}

		//check if the groupID exists in the map, if not add it to the map
		if _, exists := recordsMap[groupID]; !exists {
			recordsMap[groupID] = make(map[string]*Record)
		}

		//create a key of string type to store the groupID and accountID
		key := fmt.Sprintf("%d-%d", groupID, accountID)

		//check if the key exists in the map, if not add it to the map
		if _, exists := recordsMap[groupID][key]; !exists {
			recordsMap[groupID][key] = &Record{
				GroupID:   groupID,
				AccountID: accountID,
				GroupName: groupName,
				Address:   address,
				Services:  make(map[string]float64),
				Total:     0,
				Taxes:     taxes,
				Discount:  discount,
			}
		}

		recordsMap[groupID][key].Services[servName] += total
		recordsMap[groupID][key].Total += total
		serviceNames[servName] = true
	}

	sheetName := "GOB Breakdown"
	f.SetSheetName("Sheet1", sheetName)

	headers := []string{"No.", "Customer Name", "Address"}
	serviceList := make([]string, 0, len(serviceNames))
	for serv := range serviceNames {
		serviceList = append(serviceList, serv)
	}
	headers = append(headers, serviceList...)
	headers = append(headers, "Total", "GST", "Subtotal")

	//.......................GOB BREAKDOWN STYLE......................................................
	GOBstyle, _ := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{Bold: true, Color: "#FFFFFF"},
		Fill: excelize.Fill{Type: "pattern", Color: []string{"#4F81BD"}, Pattern: 1},
	})
	GOBtotalStyle, _ := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{Bold: true, Color: "#4F81BD"},
		Border: []excelize.Border{
			{Type: "top", Color: "#4F81BD", Style: 5},
			{Type: "bottom", Color: "#4F81BD", Style: 5},
			// {Type: "left", Color: "#4F81BD", Style: 5},
			// {Type: "right", Color: "#4F81BD", Style: 5}
		},
	})

	groups_style, _ := f.NewStyle(&excelize.Style{

		Font: &excelize.Font{Bold: true, Color: "#000000"},
	})

	overall_total, _ := f.NewStyle(&excelize.Style{

		Font: &excelize.Font{Bold: true, Color: "#000000"},
		Border: []excelize.Border{
			{Type: "top", Color: "#000000", Style: 5},
			{Type: "bottom", Color: "#000000", Style: 5},
		},
	})
	//....................................................................................

	for i, header := range headers {
		col := string('A' + i)
		f.SetCellValue(sheetName, col+"1", header)
		f.SetColWidth(sheetName, col, col, 20)
		f.SetCellStyle(sheetName, col+"1", col+"1", GOBstyle)
	}

	rowNum := 2
	var overallTotals []float64
	var overallGSTs []float64
	var overallSubtotals []float64

	for _, groupRecords := range recordsMap {
		groupStartRow := rowNum // Track the start row for the current group
		groupTotal := 0.0
		groupGST := 0.0
		groupSubtotal := 0.0

		for _, record := range groupRecords {
			f.SetCellValue(sheetName, fmt.Sprintf("A%d", rowNum), rowNum-1)
			f.SetCellValue(sheetName, fmt.Sprintf("B%d", rowNum), record.GroupName)
			f.SetCellValue(sheetName, fmt.Sprintf("C%d", rowNum), record.Address)

			colIndex := 3
			for _, serv := range serviceList {
				col := string('A' + colIndex)
				if value, exists := record.Services[serv]; exists { // Check if the service exists for the current record and set the value
					f.SetCellValue(sheetName, fmt.Sprintf("%s%d", col, rowNum), value)
				}
				colIndex++
			}
			gst := record.Total * 0.125
			subtotal := record.Total + gst

			f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+colIndex), rowNum), record.Total)
			f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+colIndex+1), rowNum), gst)
			f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+colIndex+2), rowNum), subtotal)

			groupTotal += record.Total
			groupGST += gst
			groupSubtotal += subtotal

			rowNum++

			f.SetCellStyle(sheetName, fmt.Sprintf("A%d", groupStartRow), fmt.Sprintf("%s%d", string('A'+colIndex+2), rowNum-1), groups_style) // added this to sytle the group border
		}

		// Add total row for the current group
		lastRow := rowNum + 1
		f.SetCellValue(sheetName, "C"+strconv.Itoa(lastRow), "Total:")

		for i := 0; i < len(headers)-3; i++ {
			col := string('D' + i)
			sumFormula := fmt.Sprintf("SUM(%s%d:%s%d)", col, groupStartRow, col, lastRow-1)
			f.SetCellFormula(sheetName, fmt.Sprintf("%s%d", col, lastRow), sumFormula)
			f.SetCellStyle(sheetName, fmt.Sprintf("C%d", lastRow), fmt.Sprintf("%s%d", col, lastRow), GOBtotalStyle)
		}

		// Save group totals to overall totals
		overallTotals = append(overallTotals, groupTotal)
		overallGSTs = append(overallGSTs, groupGST)
		overallSubtotals = append(overallSubtotals, groupSubtotal)

		// Move to the next row after the total row
		rowNum = lastRow + 2
	}

	// Add overall totals row
	overallTotalSum := 0.0
	overallGSTSum := 0.0
	overallSubtotalSum := 0.0

	for _, total := range overallTotals {
		overallTotalSum += total
	}
	for _, gst := range overallGSTs {
		overallGSTSum += gst
	}
	for _, subtotal := range overallSubtotals {
		overallSubtotalSum += subtotal
	}

	finalRow := rowNum + 1
	//f.SetCellValue(sheetName, "C"+strconv.Itoa(finalRow), "Overall Total:")
	f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+len(headers)-3), finalRow), overallTotalSum)
	f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+len(headers)-2), finalRow), overallGSTSum)
	f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+len(headers)-1), finalRow), overallSubtotalSum)
	f.SetCellStyle(sheetName, fmt.Sprintf("%s%d", string('A'+len(headers)-3), finalRow), fmt.Sprintf("%s%d", string('A'+len(headers)-1), finalRow), overall_total)

	// ----------------------GOB SUMMARY SHEET-------------------------------------------------------------------------------------
	summarySheetName := "GOB Summary"
	f.NewSheet(summarySheetName)

	summaryHeaders := []string{"No.", "Customer Name", "Address", "Total", "GST", "Sub-Total"}
	for i, header := range summaryHeaders {
		col := string('A' + i)
		f.SetCellValue(summarySheetName, col+"1", header)
		f.SetColWidth(summarySheetName, col, col, 25)
	}

	// Create styles for header and total rows
	summaryheaderStyle, _ := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{Bold: true, Color: "#FFFFFF"},
		Fill: excelize.Fill{Type: "pattern", Color: []string{"#4F81BD"}, Pattern: 1},
	})

	f.SetCellStyle(summarySheetName, "A1", "F1", summaryheaderStyle)

	groupTotals := make(map[string]float64)
	groupAddresses := make(map[string]string)

	for _, groupRecords := range recordsMap {
		for _, record := range groupRecords {
			groupTotals[record.GroupName] += record.Total
			groupAddresses[record.GroupName] = record.Address
		}
	}

	summaryRowNum := 2
	for groupName, total := range groupTotals {
		gst := total * 0.125
		subtotal := total + gst

		f.SetCellValue(summarySheetName, fmt.Sprintf("A%d", summaryRowNum), summaryRowNum-1)
		f.SetCellValue(summarySheetName, fmt.Sprintf("B%d", summaryRowNum), groupName)
		f.SetCellValue(summarySheetName, fmt.Sprintf("C%d", summaryRowNum), groupAddresses[groupName])
		f.SetCellValue(summarySheetName, fmt.Sprintf("D%d", summaryRowNum), total)
		f.SetCellValue(summarySheetName, fmt.Sprintf("E%d", summaryRowNum), gst)
		f.SetCellValue(summarySheetName, fmt.Sprintf("F%d", summaryRowNum), subtotal)

		summaryRowNum++
	}

	// ----------------------Add the total column to last row GOB SUMMARY----------------------------------------------
	summarylastRow := summaryRowNum + 1
	//f.SetCellValue(summarySheetName, "C"+strconv.Itoa(summarylastRow), "Total:")

	// Add column sums
	for i := 0; i < len(summaryHeaders)-3; i++ {
		col := string('D' + i)
		sumFormula := fmt.Sprintf("SUM(%s2:%s%d)", col, col, summarylastRow-2)
		f.SetCellFormula(summarySheetName, fmt.Sprintf("%s%d", col, summarylastRow), sumFormula)
	}

	// Apply total style
	f.SetCellStyle(summarySheetName, fmt.Sprintf("C%d", summarylastRow), fmt.Sprintf("F%d", summarylastRow), overall_total)

	//------------------------------NEW DEDINT ACCOUNT ACTIVATIONS TABLE---------------------------------------------------------------------------------------

	//type can either be 1:Corporate, 2:GOB. i will need to create 2 slice and from those save based on the customer type.
	//pass the slice for GOB to be handles in the GOB sheet
	//pass the type for corporate to be handled in the corporate sheet

	NuevoActivations := `
	select
	dw.cust_id,
	concat(c.first_name, ' ', c.last_name) as customer_name,	
	dw.plan_name,
	c.address,
	dw2.description,
	dw.date_submit,
	dw.install_date
from
	dedint_workorder dw
join
	customer c
on
	dw.cust_id = c.cust_id
join
	dedint_workorderstatus dw2
on
	dw.status = dw2.status_id
where
	(dw.date_submit >= '2024-11-01' and dw.install_date <= '2024-11-30')
	and c.customertype = 2 -- 0:Person, 1:Corporate, 2:GOB
	and dw.status in ('1','3')
order by
	description desc;
	`

	newactivations, err := db.Query(NuevoActivations)
	if err != nil {
		log.Fatal(err)
	}
	defer newactivations.Close()

	var newAcctSlice []NewAccounts
	for newactivations.Next() {
		var NewAcc NewAccounts

		err := newactivations.Scan(&NewAcc.custID, &NewAcc.customerName, &NewAcc.planName, &NewAcc.address, &NewAcc.description, &NewAcc.dateSubmit, &NewAcc.installDate)
		if err != nil {
			log.Fatalf("error scanning new active accounts %v", err)
		}

		newAcctSlice = append(newAcctSlice, NewAcc)

	}

	f.SetCellValue(summarySheetName, fmt.Sprintf("D%d", summarylastRow+3), "New Account Activations")
	NewAcctHeaders := []string{"No.", "CustomerID", "Customer Name", "Plan Name", "Address", "Description", "Date Submit", "Install Date"}
	for i, NAheader := range NewAcctHeaders {
		col := string('A' + i)
		cellRef := fmt.Sprintf("%s%d", col, summarylastRow+4)
		f.SetCellValue(summarySheetName, cellRef, NAheader)
		f.SetColWidth(summarySheetName, col, col, 25)
		f.SetCellStyle(summarySheetName, cellRef, cellRef, summaryheaderStyle)
	}

	NEWACTIVATION := summarylastRow + 5
	for _, new := range newAcctSlice {

		f.SetCellValue(summarySheetName, fmt.Sprintf("A%d", NEWACTIVATION), NEWACTIVATION-1)
		f.SetCellValue(summarySheetName, fmt.Sprintf("B%d", NEWACTIVATION), new.custID)
		f.SetCellValue(summarySheetName, fmt.Sprintf("C%d", NEWACTIVATION), new.customerName)
		f.SetCellValue(summarySheetName, fmt.Sprintf("D%d", NEWACTIVATION), new.planName)
		f.SetCellValue(summarySheetName, fmt.Sprintf("E%d", NEWACTIVATION), new.address)
		f.SetCellValue(summarySheetName, fmt.Sprintf("F%d", NEWACTIVATION), new.description)
		f.SetCellValue(summarySheetName, fmt.Sprintf("G%d", NEWACTIVATION), new.dateSubmit)
		f.SetCellValue(summarySheetName, fmt.Sprintf("H%d", NEWACTIVATION), new.installDate)

		NEWACTIVATION++
	}
	//------------------------------------------------------------------------------------------------------------------------------------------------------------------

}

func BusinessAccounts() {

	Business_Summary := "Business Summary"
	Business_breakdown := "Business Breakdown"
	f.NewSheet(Business_Summary)
	f.NewSheet(Business_breakdown)

}
