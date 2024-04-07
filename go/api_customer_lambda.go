package openapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

// Function to retrieve database credentials from Secrets Manager
func getDBCredentials() (DBCredentials, error) {
	var dbCredentials DBCredentials

	region := "eu-central-1"
	secretName := "prod/catInsurance/mysql"

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
	)
	if err != nil {
		return DBCredentials{}, fmt.Errorf("failed to load AWS configuration: %v", err)
	}

	svc := secretsmanager.NewFromConfig(cfg)
	input := &secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(secretName),
		VersionStage: aws.String("AWSCURRENT"),
	}

	result, err := svc.GetSecretValue(context.TODO(), input)
	if err != nil {
		return DBCredentials{}, fmt.Errorf("failed to retrieve database credentials from Secrets Manager: %v", err)
	}

	secretString := *result.SecretString

	// Parse the JSON secret string
	if err := json.Unmarshal([]byte(secretString), &dbCredentials); err != nil {
		return DBCredentials{}, fmt.Errorf("error parsing database credentials: %v", err)
	}

	return dbCredentials, nil
}

// Use getDBCredentials function to retrieve database credentials
func connectToDB() (*sql.DB, error) {
	// Retrieve database credentials
	dbCredentials, err := getDBCredentials()
	if err != nil {
		return nil, err
	}

	// Format the DSN for connecting to the MySQL database
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/meowmeddb", dbCredentials.Username, dbCredentials.Password, dbCredentials.Host, dbCredentials.Port)

	// Connect to the database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %v", err)
	}

	return db, nil
}

func HandleCustomersCustomerIdGet(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	customerID := req.PathParameters["customerId"]

	if customerID == "" {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       "Missing customerId parameter",
		}, nil
	}

	db, err := connectToDB()
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Error connecting to database",
		}, nil
	}
	defer db.Close()

	var customer CustomerRes
	customer.Address = &Address{}
	customer.BankDetails = &BankDetails{}

	err = db.QueryRowContext(ctx,  `
    SELECT
        c.id, c.firstName, c.lastName, COALESCE(c.title, '') AS title, c.familyStatus, c.birthDate,
        c.socialSecurityNumber, c.taxId, c.jobStatus,
        a.street, a.houseNumber, a.zipCode, a.city,
        b.iban, b.bic, b.name AS bankName
    FROM
        Customer AS c
    JOIN
        Address AS a ON c.addressId = a.id
    JOIN
        BankDetails AS b ON c.bankDetailsId = b.id
    WHERE
        c.id = ?`, customerID).Scan(
			&customer.Id, &customer.FirstName, &customer.LastName, &customer.Title, &customer.FamilyStatus, &customer.BirthDate,
			&customer.SocialSecurityNumber, &customer.TaxId, &customer.JobStatus,
			&customer.Address.Street, &customer.Address.HouseNumber, &customer.Address.ZipCode, &customer.Address.City,
			&customer.BankDetails.Iban, &customer.BankDetails.Bic, &customer.BankDetails.Name))
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Error retrieving customer details",
		}, nil
	}

	responseJSON, err := json.Marshal(customer)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Error serializing customer details",
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: string(responseJSON),
	}, nil
}

func HandleCustomersGet(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	page, pageSize, err := parseQueryParams(req.QueryStringParameters)
	if err != nil || page < 1{
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       err.Error(),
		}, nil
	}

	offset := (page - 1) * pageSize

	db, err := connectToDB()
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Error connecting to database",
		}, nil
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx,  `
    SELECT
        c.id, c.firstName, c.lastName, COALESCE(c.title, '') AS title, c.familyStatus, c.birthDate,
        c.socialSecurityNumber, c.taxId, c.jobStatus,
        a.street, a.houseNumber, a.zipCode, a.city,
        b.iban, b.bic, b.name AS bankName
    FROM
        Customer AS c
    JOIN
        Address AS a ON c.addressId = a.id
    JOIN
        BankDetails AS b ON c.bankDetailsId = b.id
    ORDER BY c.id ASC
    LIMIT ? OFFSET ?`, pageSize, offset)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Error retrieving customer details",
		}, nil
	}
	defer rows.Close()

	// Construct slice to hold customer details
	var customers []CustomerRes

	// Iterate over the rows and populate the customers slice
	for rows.Next() {
		var customer CustomerRes
		customer.Address = &Address{}
		customer.BankDetails = &BankDetails{}
		if err := rows.Scan(
			&customer.Id, &customer.FirstName, &customer.LastName, &customer.Title, &customer.FamilyStatus, &customer.BirthDate,
			&customer.SocialSecurityNumber, &customer.TaxId, &customer.JobStatus,
			&customer.Address.Street, &customer.Address.HouseNumber, &customer.Address.ZipCode, &customer.Address.City,
			&customer.BankDetails.Iban, &customer.BankDetails.Bic, &customer.BankDetails.Name,
		); err != nil {
			return events.APIGatewayProxyResponse{
				StatusCode: 500,
				Body:       "Error scanning customer details",
			}, nil
		}

		// Append customer details to the customers slice
		customers = append(customers, customer)
	}

	if err := rows.Err(); err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Error iterating over customer details",
		}, nil
	}

	responseJSON, err := json.Marshal(customers)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Error serializing customer details",
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: string(responseJSON),
	}, nil
}

func HandleCustomersPost(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var newCustomerReq CustomerReq
	if err := json.Unmarshal([]byte(req.Body), &newCustomerReq); err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       "Error decoding request body",
		}, nil
	}

	db, err := connectToDB()
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Error connecting to database",
		}, nil
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Error starting transaction",
		}, nil
	}

	addressID := uuid.New().String()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO Address (id, street, houseNumber, zipCode, city)
		VALUES (?, ?, ?, ?, ?)`,
		addressID, newCustomerReq.Address.Street, newCustomerReq.Address.HouseNumber, newCustomerReq.Address.ZipCode, newCustomerReq.Address.City)
	if err != nil {
		tx.Rollback()
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Error inserting into Address table",
		}, nil
	}

	bankDetailsID := uuid.New().String()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO BankDetails (id, iban, bic, name)
		VALUES (?, ?, ?, ?)`,
		bankDetailsID, newCustomerReq.BankDetails.Iban, newCustomerReq.BankDetails.Bic, newCustomerReq.BankDetails.Name)
	if err != nil {
		tx.Rollback()
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Error inserting into BankDetails table",
		}, nil
	}

	newCustomerID := uuid.New().String()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO Customer (id, firstName, lastName, title, familyStatus, birthDate, socialSecurityNumber, taxId, jobStatus, addressId, bankDetailsId)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		newCustomerID, newCustomerReq.FirstName, newCustomerReq.LastName, newCustomerReq.Title, newCustomerReq.FamilyStatus, newCustomerReq.BirthDate,
		newCustomerReq.SocialSecurityNumber, newCustomerReq.TaxId, newCustomerReq.JobStatus, addressID, bankDetailsID)
	if err != nil {
		tx.Rollback()
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Error inserting into Customer table",
		}, nil
	}

	if err := tx.Commit(); err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Error committing transaction",
		}, nil
	}

	newCustomerRes := CustomerRes{
		Id:                   newCustomerID,
		FirstName:            newCustomerReq.FirstName,
		LastName:             newCustomerReq.LastName,
		Title:                newCustomerReq.Title,
		FamilyStatus:         newCustomerReq.FamilyStatus,
		BirthDate:            newCustomerReq.BirthDate,
		SocialSecurityNumber: newCustomerReq.SocialSecurityNumber,
		TaxId:                newCustomerReq.TaxId,
		JobStatus:            newCustomerReq.JobStatus,
		Address: &Address{
			Street:      newCustomerReq.Address.Street,
			HouseNumber: newCustomerReq.Address.HouseNumber,
			ZipCode:     newCustomerReq.Address.ZipCode,
			City:        newCustomerReq.Address.City,
		},
		BankDetails: &BankDetails{
			Iban: newCustomerReq.BankDetails.Iban,
			Bic:  newCustomerReq.BankDetails.Bic,
			Name: newCustomerReq.BankDetails.Name,
		},
	}

	responseJSON, err := json.Marshal(newCustomerRes)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Error serializing customer details",
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 201,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: string(responseJSON),
	}, nil
}

func HandleCustomersCustomerIdDelete(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	// Extract customer ID from request path parameters
	customerID := request.PathParameters["customerId"]

	if customerID == "" {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Body:       "Missing customerId parameter",
		}, nil
	}

	// Retrieve database connection
	db, err := connectToDB()
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       fmt.Sprintf("Error connecting to database: %v", err),
		}, nil
	}
	defer db.Close()

	var addressID, bankDetailsID string

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       fmt.Sprintf("Error starting transaction: %v", err),
		}, nil
	}

	// Save address ID and bank details ID
	err = tx.QueryRowContext(ctx, `
		SELECT addressId, bankDetailsId FROM Customer WHERE id = ?`, customerID).Scan(&addressID, &bankDetailsID)
	if err != nil {
		tx.Rollback()
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       fmt.Sprintf("Error retrieving customer details: %v", err),
		}, nil
	}

	// Delete associated contracts
	_, err = tx.ExecContext(ctx, `
		DELETE FROM Contract WHERE customerId = ?`, customerID)
	if err != nil {
		tx.Rollback()
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       fmt.Sprintf("Error deleting associated contracts: %v", err),
		}, nil
	}

	// Delete the customer
	_, err = tx.ExecContext(ctx, `
		DELETE FROM Customer WHERE id = ?`, customerID)
	if err != nil {
		tx.Rollback()
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       fmt.Sprintf("Error deleting customer: %v", err),
		}, nil
	}

	// Delete customer's address
	_, err = tx.ExecContext(ctx, `
		DELETE FROM Address WHERE id = ?`, addressID)
	if err != nil {
		tx.Rollback()
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       fmt.Sprintf("Error deleting customer's address: %v", err),
		}, nil
	}

	// Delete customer's bank details
	_, err = tx.ExecContext(ctx, `
		DELETE FROM BankDetails WHERE id = ?`, bankDetailsID)
	if err != nil {
		tx.Rollback()
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       fmt.Sprintf("Error deleting customer's bank details: %v", err),
		}, nil
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       fmt.Sprintf("Error committing transaction: %v", err),
		}, nil
	}

	// Respond with success message
	responseBody, _ := json.Marshal("Customer deleted")
	return events.APIGatewayProxyResponse{
		StatusCode: http.StatusOK,
		Body:       string(responseBody),
	}, nil
}

func HandleCustomersSearchGet(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	// Parse query parameters
	queryParams := request.QueryStringParameters
	page, err := strconv.Atoi(queryParams["page"])
	if err != nil || page < 1 {
		page = 1 // Default to page 1 if the page parameter is missing or invalid
	}

	pageSize, err := strconv.Atoi(queryParams["pageSize"])
	if err != nil || pageSize < 1 {
		pageSize = 20 // Default page size if pageSize parameter is missing or invalid
	}

	// Build the SQL query and parameter list
	var args []interface{}
	sqlQuery := "SELECT c.id, c.firstName, c.lastName, c.title, c.familyStatus, c.birthDate, c.socialSecurityNumber, c.taxId, c.jobStatus, " +
		"a.street, a.houseNumber, a.zipCode, a.city, " +
		"b.iban, b.bic, b.name " +
		"FROM Customer c " +
		"INNER JOIN Address a ON c.addressId = a.id " +
		"INNER JOIN BankDetails b ON c.bankDetailsId = b.id " +
		"WHERE 1=1"

	if id, ok := queryParams["id"]; ok {
		sqlQuery += " AND c.id = ?"
		args = append(args, id)
	}
	if name, ok := queryParams["name"]; ok {
		sqlQuery += " AND c.firstName = ?"
		args = append(args, name)
	}
	if lastName, ok := queryParams["lastName"]; ok {
		sqlQuery += " AND c.lastName = ?"
		args = append(args, lastName)
	}
	if address, ok := queryParams["address"]; ok {
		sqlQuery += " AND a.street = ?"
		args = append(args, address)
	}

	// Add pagination to the SQL query
	sqlQuery += " LIMIT ? OFFSET ?"
	args = append(args, pageSize, (page-1)*pageSize)

	// Execute the SQL query
	db, err := connectToDB()
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Error connecting to database",
		}, nil
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       fmt.Sprintf("Error retrieving customer details: %v", err),
		}, nil
	}
	defer rows.Close()

	// Construct slice to hold customer details
	var customers []CustomerRes

	// Iterate over the rows and populate the customers slice
	for rows.Next() {
		var customer CustomerRes
		customer.Address = &Address{}
		customer.BankDetails = &BankDetails{}
		if err := rows.Scan(
			&customer.Id,
			&customer.FirstName,
			&customer.LastName,
			&customer.Title,
			&customer.FamilyStatus,
			&customer.BirthDate,
			&customer.SocialSecurityNumber,
			&customer.TaxId,
			&customer.JobStatus,
			&customer.Address.Street,
			&customer.Address.HouseNumber,
			&customer.Address.ZipCode,
			&customer.Address.City,
			&customer.BankDetails.Iban,
			&customer.BankDetails.Bic,
			&customer.BankDetails.Name); err != nil {
			return events.APIGatewayProxyResponse{
				StatusCode: http.StatusInternalServerError,
				Body:       fmt.Sprintf("Error scanning customer details: %v", err),
			}, nil
		}

		// Append customer details to the customers slice
		customers = append(customers, customer)
	}

	// Check for errors during rows iteration
	if err := rows.Err(); err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       fmt.Sprintf("Error iterating over customer details: %v", err),
		}, nil
	}

	if len(customers) == 0 {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusNoContent,
		}, nil
	}

	// Convert customers slice to JSON
	responseJSON, err := json.Marshal(customers)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       fmt.Sprintf("Error serializing customer details: %v", err),
		}, nil
	}

	// Write JSON response
	return events.APIGatewayProxyResponse{
		StatusCode: http.StatusOK,
		Body:       string(responseJSON),
		Headers: map[string]string{
			"Content-Type": "application/json; charset=UTF-8",
		},
	}, nil
}

func HandleCustomersCustomerIdPatch(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	// Extract customerId from path parameters
	customerID := request.PathParameters["customerId"]

	// Check if customerId is provided
	if customerID == "" {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Body:       "Missing customerId parameter",
		}, nil
	}

	// Read request body
	var updatedCustomerReq CustomerReq
	if err := json.Unmarshal([]byte(request.Body), &updatedCustomerReq); err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Body:       "Invalid input data",
		}, nil
	}

	// Retrieve database credentials
	db, err := connectToDB()
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Error connecting to database",
		}, err
	}
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Error starting transaction",
		}, nil
	}

	// Update Customer table
	_, err = tx.ExecContext(ctx, `
		UPDATE Customer
		SET
			firstName = ?,
			lastName = ?,
			title = ?,
			familyStatus = ?,
			birthDate = ?,
			socialSecurityNumber = ?,
			taxId = ?,
			jobStatus = ?
		WHERE
			id = ?`,
		updatedCustomerReq.FirstName, updatedCustomerReq.LastName, updatedCustomerReq.Title, updatedCustomerReq.FamilyStatus,
		updatedCustomerReq.BirthDate, updatedCustomerReq.SocialSecurityNumber, updatedCustomerReq.TaxId, updatedCustomerReq.JobStatus,
		customerID)
	if err != nil {
		tx.Rollback()
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Error updating customer details " + err.Error(),
		}, nil
	}

	// Update Address table if provided
	if updatedCustomerReq.Address != nil {
		_, err = tx.ExecContext(ctx, `
			UPDATE Address
			SET
				street = ?,
				houseNumber = ?,
				zipCode = ?,
				city = ?
			WHERE
				id = (SELECT addressId FROM Customer WHERE id = ?)`,
			updatedCustomerReq.Address.Street, updatedCustomerReq.Address.HouseNumber,
			updatedCustomerReq.Address.ZipCode, updatedCustomerReq.Address.City, customerID)
		if err != nil {
			tx.Rollback()
			return events.APIGatewayProxyResponse{
				StatusCode: http.StatusInternalServerError,
				Body:       "Error updating address details " + err.Error(),
			}, nil
		}
	}

	// Update BankDetails table if provided
	if updatedCustomerReq.BankDetails != nil {
		_, err = tx.ExecContext(ctx, `
			UPDATE BankDetails
			SET
				iban = ?,
				bic = ?,
				name = ?
			WHERE
				id = (SELECT bankDetailsId FROM Customer WHERE id = ?)`,
			updatedCustomerReq.BankDetails.Iban, updatedCustomerReq.BankDetails.Bic,
			updatedCustomerReq.BankDetails.Name, customerID)
		if err != nil {
			tx.Rollback()
			return events.APIGatewayProxyResponse{
				StatusCode: http.StatusInternalServerError,
				Body:       "Error updating bank details " + err.Error(),
			}, nil
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Error committing transaction",
		}, nil
	}

	// Respond with success message
	return events.APIGatewayProxyResponse{
		StatusCode: http.StatusOK,
		Body:       "Customer updated",
	}, nil
}

func parseQueryParams(params map[string]string) (int, int, error) {
	page, err := strconv.Atoi(params["page"])
	if err != nil || page < 1 {
		page = 1
	}
 
	pageSize, err := strconv.Atoi(params["pageSize"])
	if err != nil || pageSize < 1 {
		pageSize = 20
	}
 
	return page, pageSize, nil
}
 
func main() {
	lambda.Start(HandleCustomersCustomerIdGet)
	lambda.Start(HandleCustomersGet)
	lambda.Start(HandleCustomersPost)
	lambda.Start(HandleCustomersCustomerIdDelete)
	lambda.Start(HandleCustomersSearchGet)
	lambda.Start(HandleCustomersCustomerIdPatch)
 }