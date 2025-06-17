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

type Record struct {
	GroupID     int
	AccountID   int
	GroupName   string
	Address     string
	Services    map[string]float64
	Total       float64
	Taxes       float64
	Discount    float64
	Accounttype string
	Cycle       string
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

type BSSbalance struct {
	Balance int    `json:"balance"`
	Name    int    `json:"name"`
	DueDate string `json:"due_date"`
}

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
	var groupNametemp, addresstemp, cust_typeTemp string

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
			b.discounts,
			b.customer_type 
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
				bc.address,
				bc.customer_type
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
			var custype string

			err := rows.Scan(&groupID, &period, &cycle, &accountID, &groupName, &address, &billSource, &acctServID, &servName, &total, &taxes, &discounts, &custype)
			if err != nil {
				log.Fatalf("error scanning child services %s", err)
			}

			//assign the groupname and address to the temp variables
			groupNametemp = groupName
			addresstemp = address
			cust_typeTemp = custype

			//fmt.Println(groupID, period, cycle, accountID, groupName, address, billSource, acctServID, servName, total, taxes, discounts)

			//insert the child services into the consolidated_bills_summary table

			insetChildServices := ` INSERT INTO public.consolidated_bills_summary (group_id,"period","cycle",account_id,group_name,address,bill_source,acct_serv_id,serv_name,total,taxes,discount,account_type,created_at)
											VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW())`

			_, err = db.ExecContext(context.Background(), insetChildServices, groupID, period, cycle, accountID, groupName, address, billSource, acctServID, servName, total, taxes, discounts, custype)

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

			insertBSSservices := ` INSERT INTO public.consolidated_bills_summary (group_id,"period","cycle",account_id,group_name,address,bill_source,acct_serv_id,serv_name,total,taxes,discount,account_type,created_at)
								VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW())`

			_, err = db.ExecContext(context.Background(), insertBSSservices, groupID, invoice_date, billingcycle, customerID, groupNametemp, addresstemp, "bss", invoiceID, "Postpaid", total, taxes, discount, cust_typeTemp)

			if err != nil {
				log.Fatalf("%v", err)
			}

		}

		//-----------------------------------------------------------------------------------------------

	} //end of parentID loop

	println("Child services inserted successfully")

	//call the generate report function

	generateReport(db)

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
        SELECT group_id, account_id, group_name, address, serv_name, total, taxes, discount, account_type,cycle
        FROM consolidated_bills_summary
        ORDER BY group_id
    `

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var GOBRecords []Record
	var BusinessRecords []Record
	recordsMap := []Record{}
	serviceNames := make(map[string]bool)

	for rows.Next() {

		var servName string
		var record Record

		if err := rows.Scan(&record.GroupID, &record.AccountID, &record.GroupName, &record.Address, &servName, &record.Total, &record.Taxes, &record.Discount, &record.Accounttype, &record.Cycle); err != nil {
			log.Fatal(err)
		}

		if record.Services == nil {
			record.Services = make(map[string]float64)
		}

		record.Services[servName] += record.Total

		recordsMap = append(recordsMap, record)

		serviceNames[servName] = true

	}

	for _, record := range recordsMap {
		if record.Accounttype == "GOB" {
			GOBRecords = append(GOBRecords, record)
		} else {
			BusinessRecords = append(BusinessRecords, record)
		}
	}

	sheetName := "GOB Breakdown"
	f.SetSheetName("Sheet1", sheetName)

	headers := []string{"No.", "Customer Name", "Address"}
	serviceList := make([]string, 0, len(serviceNames))
	for serv := range serviceNames {
		serviceList = append(serviceList, serv)
	}

	headers = append(headers, serviceList...)
	headers = append(headers, "Discount", "Total", "GST", "Subtotal")

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
		},
		Alignment: &excelize.Alignment{
			Horizontal: "center",
			Vertical:   "center",
		},
	})

	overall_total, _ := f.NewStyle(&excelize.Style{

		Font: &excelize.Font{Bold: true, Color: "#000000"},
		Border: []excelize.Border{
			{Type: "top", Color: "#000000", Style: 5},
			{Type: "bottom", Color: "#000000", Style: 5},
		},
	})

	centeredDashStyle, _ := f.NewStyle(&excelize.Style{
		Alignment: &excelize.Alignment{
			Horizontal: "center",
			Vertical:   "center",
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
	groupStartRow := rowNum
	groupTotal := 0.0
	groupGST := 0.0
	groupSubtotal := 0.0
	var overallTotals []float64
	var overallGSTs []float64
	var overallSubtotals []float64
	var prevGroupID int

	//....................................................GOB ACCOUNTS......................................................
	for i, record := range GOBRecords {

		total := 0.0
		for _, amount := range record.Services {
			total += amount
		}
		total -= record.Discount
		record.Total = total

		currentGroupID := record.GroupID

		if i > 0 && currentGroupID != prevGroupID {
			// Insert group total row before starting a new group
			lastRow := rowNum
			f.SetCellValue(sheetName, fmt.Sprintf("B%d", rowNum), "Total:")

			for i := 0; i < len(serviceList)+1; i++ {
				col := string('D' + i)
				sumFormula := fmt.Sprintf("SUM(%s%d:%s%d)", col, groupStartRow, col, lastRow-1)
				f.SetCellFormula(sheetName, fmt.Sprintf("%s%d", col, rowNum), sumFormula)
				f.SetCellStyle(sheetName, fmt.Sprintf("D%d", lastRow), fmt.Sprintf("%s%d", col, lastRow), GOBtotalStyle)
			}

			// Write group totals
			colIndex := 3 + len(serviceList) + 1
			f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+colIndex), rowNum), -record.Discount)
			f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+colIndex), rowNum), groupTotal)
			f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+colIndex+1), rowNum), groupGST)
			f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+colIndex+2), rowNum), groupSubtotal)
			f.SetCellStyle(sheetName, fmt.Sprintf("%s%d", string('A'+colIndex), rowNum), fmt.Sprintf("%s%d", string('A'+colIndex+2), rowNum), GOBtotalStyle)

			overallTotals = append(overallTotals, groupTotal)
			overallGSTs = append(overallGSTs, groupGST)
			overallSubtotals = append(overallSubtotals, groupSubtotal)

			rowNum += 2                                         // Move to next row for new group
			groupStartRow = rowNum                              // Reset start row for new group
			groupTotal, groupGST, groupSubtotal = 0.0, 0.0, 0.0 // Reset group totals
		}

		// Store row data
		f.SetCellValue(sheetName, fmt.Sprintf("A%d", rowNum), rowNum-1)
		f.SetCellValue(sheetName, fmt.Sprintf("B%d", rowNum), record.GroupName)
		f.SetCellValue(sheetName, fmt.Sprintf("C%d", rowNum), record.Address)

		colIndex := 3
		for _, serv := range serviceList {
			col := string('A' + colIndex)
			cell := fmt.Sprintf("%s%d", col, rowNum)
			value, exists := record.Services[serv]
			if !exists || value == 0 {
				f.SetCellValue(sheetName, cell, "$")
				f.SetCellStyle(sheetName, cell, cell, centeredDashStyle)
			} else {
				f.SetCellValue(sheetName, cell, value)
				f.SetCellStyle(sheetName, cell, cell, centeredDashStyle)
			}
			colIndex++
		}

		gst := record.Total * 0.125
		subtotal := record.Total + gst

		cell := fmt.Sprintf("%s%d", string('A'+colIndex), rowNum)
		if record.Discount == 0 {
			f.SetCellValue(sheetName, cell, "$")
			f.SetCellStyle(sheetName, cell, cell, centeredDashStyle)
		} else {
			f.SetCellValue(sheetName, cell, -record.Discount)
			f.SetCellStyle(sheetName, cell, cell, centeredDashStyle)
		}

		f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+colIndex+1), rowNum), record.Total)
		f.SetCellStyle(sheetName, fmt.Sprintf("%s%d", string('A'+colIndex+1), rowNum), fmt.Sprintf("%s%d", string('A'+colIndex+1), rowNum), centeredDashStyle)

		f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+colIndex+2), rowNum), gst)
		f.SetCellStyle(sheetName, fmt.Sprintf("%s%d", string('A'+colIndex+2), rowNum), fmt.Sprintf("%s%d", string('A'+colIndex+2), rowNum), centeredDashStyle)

		f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+colIndex+3), rowNum), subtotal)
		f.SetCellStyle(sheetName, fmt.Sprintf("%s%d", string('A'+colIndex+3), rowNum), fmt.Sprintf("%s%d", string('A'+colIndex+3), rowNum), centeredDashStyle)

		// Accumulate group totals
		groupTotal += record.Total
		groupGST += gst
		groupSubtotal += subtotal

		// Update previous group ID for next iteration
		prevGroupID = currentGroupID

		rowNum++

	}

	// Final group total (for the last group)
	if len(GOBRecords) > 0 {
		lastRow := rowNum
		f.SetCellValue(sheetName, fmt.Sprintf("B%d", rowNum), "Total:")

		for i := 0; i < len(serviceList)+1; i++ {
			col := string('D' + i)
			sumFormula := fmt.Sprintf("SUM(%s%d:%s%d)", col, groupStartRow, col, lastRow-1)
			f.SetCellFormula(sheetName, fmt.Sprintf("%s%d", col, rowNum), sumFormula)
			f.SetCellStyle(sheetName, fmt.Sprintf("D%d", lastRow), fmt.Sprintf("%s%d", col, lastRow), GOBtotalStyle)

		}

		colIndex := 3 + len(serviceList) + 1
		f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+colIndex), rowNum), groupTotal)
		f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+colIndex+1), rowNum), groupGST)
		f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+colIndex+2), rowNum), groupSubtotal)
		f.SetCellStyle(sheetName, fmt.Sprintf("%s%d", string('A'+colIndex), rowNum), fmt.Sprintf("%s%d", string('A'+colIndex+2), rowNum), GOBtotalStyle)

		//Add lastgroup total to overall totals
		overallTotals = append(overallTotals, groupTotal)
		overallGSTs = append(overallGSTs, groupGST)
		overallSubtotals = append(overallSubtotals, groupSubtotal)

		rowNum++

	}

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
	f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+len(headers)-3), finalRow), overallTotalSum)
	f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+len(headers)-2), finalRow), overallGSTSum)
	f.SetCellValue(sheetName, fmt.Sprintf("%s%d", string('A'+len(headers)-1), finalRow), overallSubtotalSum)
	f.SetCellStyle(sheetName, fmt.Sprintf("%s%d", string('A'+len(headers)-3), finalRow), fmt.Sprintf("%s%d", string('A'+len(headers)-1), finalRow), overall_total)

	// -----------------------------------GOB SUMMARY SHEET---------------------------------------------------
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

	for _, govRecords := range GOBRecords {

		govRecords.Total -= govRecords.Discount
		groupTotals[govRecords.GroupName] += govRecords.Total
		groupAddresses[govRecords.GroupName] = govRecords.Address

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

	// Add column sums
	for i := 0; i < len(summaryHeaders)-3; i++ {
		col := string('D' + i)
		sumFormula := fmt.Sprintf("SUM(%s2:%s%d)", col, col, summarylastRow-2)
		f.SetCellFormula(summarySheetName, fmt.Sprintf("%s%d", col, summarylastRow), sumFormula)
	}

	// Apply total style
	f.SetCellStyle(summarySheetName, fmt.Sprintf("D%d", summarylastRow), fmt.Sprintf("F%d", summarylastRow), overall_total)

	// BusinessAccounts(db, BusinessRecords, serviceNames)
}

// ....................................................BUSINESS ACCOUNTS......................................................

func BusinessAccounts(db *sql.DB, BusinessRecords []Record, serviceNames map[string]bool) {

	//.......................BUSNIESS BREAKDOWN STYLE......................................................
	BUSstyle, _ := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{Bold: true, Color: "#FFFFFF"},
		Fill: excelize.Fill{Type: "pattern", Color: []string{"#4F81BD"}, Pattern: 1},
	})
	BUStotalStyle, _ := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{Bold: true, Color: "#4F81BD"},
		Border: []excelize.Border{
			{Type: "top", Color: "#4F81BD", Style: 5},
			{Type: "bottom", Color: "#4F81BD", Style: 5},
		},
	})

	groups_style, _ := f.NewStyle(&excelize.Style{

		Font: &excelize.Font{Bold: true, Color: "#000000"},
	})

	BUSoverall_total, _ := f.NewStyle(&excelize.Style{

		Font: &excelize.Font{Bold: true, Color: "#000000"},
		Border: []excelize.Border{
			{Type: "top", Color: "#000000", Style: 5},
			{Type: "bottom", Color: "#000000", Style: 5},
		},
	})
	//....................................................................................

	rowNum := 2
	groupStartRow := rowNum
	groupTotal := 0.0
	groupGST := 0.0
	groupSubtotal := 0.0
	var overallTotals []float64
	var overallGSTs []float64
	var overallSubtotals []float64
	var prevGroupID int

	BUSsheetName := "Business Breakdown"
	f.NewSheet(BUSsheetName)

	Busheaders := []string{"No.", "Customer Name", "Address"}
	BUSserviceList := make([]string, 0, len(serviceNames))
	for serv := range serviceNames {
		BUSserviceList = append(BUSserviceList, serv)
	}
	Busheaders = append(Busheaders, BUSserviceList...)
	Busheaders = append(Busheaders, "Discount", "Total", "GST", "Subtotal") //added discount

	for i, header := range Busheaders {
		col := string('A' + i)
		f.SetCellValue(BUSsheetName, col+"1", header)
		f.SetColWidth(BUSsheetName, col, col, 20)
		f.SetCellStyle(BUSsheetName, col+"1", col+"1", BUSstyle)
	}

	for i, BUSrecord := range BusinessRecords {

		// Calculate total amount with discount
		total := 0.0
		for _, amount := range BUSrecord.Services {
			total += amount
		}
		total -= BUSrecord.Discount
		BUSrecord.Total = total

		currentGroupID := BUSrecord.GroupID

		// Check if groupID has changed (excluding the first iteration)
		if i > 0 && currentGroupID != prevGroupID {
			// Insert group total row before starting a new group
			lastRow := rowNum
			f.SetCellValue(BUSsheetName, fmt.Sprintf("B%d", rowNum), "Total:")

			for i := 0; i < len(BUSserviceList)+1; i++ { //DIS
				col := string('D' + i)
				sumFormula := fmt.Sprintf("SUM(%s%d:%s%d)", col, groupStartRow, col, lastRow-1)
				f.SetCellFormula(BUSsheetName, fmt.Sprintf("%s%d", col, rowNum), sumFormula)
				f.SetCellStyle(BUSsheetName, fmt.Sprintf("D%d", lastRow), fmt.Sprintf("%s%d", col, lastRow), BUStotalStyle)
			}

			// Write group totals
			colIndex := 3 + len(BUSserviceList) + 1                                                                //DIS
			f.SetCellValue(BUSsheetName, fmt.Sprintf("%s%d", string('A'+colIndex-1), rowNum), -BUSrecord.Discount) //DIS
			f.SetCellValue(BUSsheetName, fmt.Sprintf("%s%d", string('A'+colIndex+1), rowNum), groupTotal)
			f.SetCellValue(BUSsheetName, fmt.Sprintf("%s%d", string('A'+colIndex+2), rowNum), groupGST)
			f.SetCellValue(BUSsheetName, fmt.Sprintf("%s%d", string('A'+colIndex+3), rowNum), groupSubtotal)
			f.SetCellStyle(BUSsheetName, fmt.Sprintf("%s%d", string('A'+colIndex), rowNum), fmt.Sprintf("%s%d", string('A'+colIndex+2), rowNum), BUStotalStyle)
			f.SetCellStyle(BUSsheetName, fmt.Sprintf("A%d", groupStartRow), fmt.Sprintf("%s%d", string('A'+colIndex+2), rowNum-1), groups_style)

			overallTotals = append(overallTotals, groupTotal)
			overallGSTs = append(overallGSTs, groupGST)
			overallSubtotals = append(overallSubtotals, groupSubtotal)

			rowNum += 2                                         // Move to next row for new group
			groupStartRow = rowNum                              // Reset start row for new group
			groupTotal, groupGST, groupSubtotal = 0.0, 0.0, 0.0 // Reset group totals
		}

		// Store row data
		f.SetCellValue(BUSsheetName, fmt.Sprintf("A%d", rowNum), rowNum-1)
		f.SetCellValue(BUSsheetName, fmt.Sprintf("B%d", rowNum), BUSrecord.GroupName)
		f.SetCellValue(BUSsheetName, fmt.Sprintf("C%d", rowNum), BUSrecord.Address)

		colIndex := 3 //DIS
		for _, serv := range BUSserviceList {
			col := string('A' + colIndex)
			if value, exists := BUSrecord.Services[serv]; exists {
				f.SetCellValue(BUSsheetName, fmt.Sprintf("%s%d", col, rowNum), value)
			}
			colIndex++
		}

		gst := BUSrecord.Total * 0.125
		subtotal := BUSrecord.Total + gst

		f.SetCellValue(BUSsheetName, fmt.Sprintf("%s%d", string('A'+colIndex), rowNum), -BUSrecord.Discount) // Discounts
		f.SetCellValue(BUSsheetName, fmt.Sprintf("%s%d", string('A'+colIndex+1), rowNum), BUSrecord.Total)
		f.SetCellValue(BUSsheetName, fmt.Sprintf("%s%d", string('A'+colIndex+2), rowNum), gst)
		f.SetCellValue(BUSsheetName, fmt.Sprintf("%s%d", string('A'+colIndex+3), rowNum), subtotal)

		// Accumulate group totals
		groupTotal += BUSrecord.Total
		groupGST += gst
		groupSubtotal += subtotal

		// Update previous group ID for next iteration
		prevGroupID = currentGroupID

		rowNum++ // Move to next row
	}

	// Final group total (for the last group)
	if len(BusinessRecords) > 0 {
		lastRow := rowNum
		f.SetCellValue(BUSsheetName, fmt.Sprintf("B%d", rowNum), "Total:")

		for i := 0; i < len(BUSserviceList)+1; i++ { //DIS
			col := string('D' + i)
			sumFormula := fmt.Sprintf("SUM(%s%d:%s%d)", col, groupStartRow, col, lastRow-1)
			f.SetCellFormula(BUSsheetName, fmt.Sprintf("%s%d", col, rowNum), sumFormula)
			f.SetCellStyle(BUSsheetName, fmt.Sprintf("D%d", lastRow), fmt.Sprintf("%s%d", col, lastRow), BUStotalStyle)
		}

		colIndex := 3 + len(BUSserviceList) + 1 //DIS
		//f.SetCellValue(BUSsheetName, fmt.Sprintf("%s%d", string('A'+colIndex-1), rowNum), BUSdiscount) //DIS
		f.SetCellValue(BUSsheetName, fmt.Sprintf("%s%d", string('A'+colIndex), rowNum), groupTotal)
		f.SetCellValue(BUSsheetName, fmt.Sprintf("%s%d", string('A'+colIndex+1), rowNum), groupGST)
		f.SetCellValue(BUSsheetName, fmt.Sprintf("%s%d", string('A'+colIndex+2), rowNum), groupSubtotal)
		f.SetCellStyle(BUSsheetName, fmt.Sprintf("%s%d", string('A'+colIndex), rowNum), fmt.Sprintf("%s%d", string('A'+colIndex+2), rowNum), BUStotalStyle)
		//f.SetCellStyle(BUSsheetName, fmt.Sprintf("A%d", groupStartRow), fmt.Sprintf("%s%d", string('A'+colIndex+2), rowNum-1), groups_style)

		overallTotals = append(overallTotals, groupTotal)
		overallGSTs = append(overallGSTs, groupGST)
		overallSubtotals = append(overallSubtotals, groupSubtotal)

		rowNum++ // Move past the final total row
	}

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
	f.SetCellValue(BUSsheetName, fmt.Sprintf("%s%d", string('A'+len(Busheaders)-3), finalRow), overallTotalSum)
	f.SetCellValue(BUSsheetName, fmt.Sprintf("%s%d", string('A'+len(Busheaders)-2), finalRow), overallGSTSum)
	f.SetCellValue(BUSsheetName, fmt.Sprintf("%s%d", string('A'+len(Busheaders)-1), finalRow), overallSubtotalSum)
	f.SetCellStyle(BUSsheetName, fmt.Sprintf("%s%d", string('A'+len(Busheaders)-3), finalRow), fmt.Sprintf("%s%d", string('A'+len(Busheaders)-1), finalRow), BUSoverall_total)

}
