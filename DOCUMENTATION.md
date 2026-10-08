# Nishan Rehab Center Hospital Management System (HMS)

## 1. Introduction
**Nishan Rehab Center HMS** is a robust, full-stack Hospital Management and Customer Relationship Management (CRM) system designed for Nishan Rehab Center. It streamlines patient admissions, financial tracking, medical records, and daily administrative operations.

The system is built as a highly interactive, single-page application (SPA) focused on usability, data integrity, and role-based access.

---

## 2. Technical Stack
### Backend
- **Framework**: Python 3.x with Flask
- **Database**: MongoDB (via Flask-PyMongo)
- **Authentication**: Session-based with Werkzeug password hashing
- **Security**: 
  - `Flask-Talisman` for HTTP headers security
  - `Flask-Limiter` for rate limiting (auth routes)
  - `Flask-CORS` for Cross-Origin Resource Sharing
  - `itsdangerous` for time-bound password reset tokens
- **Email**: SMTP integration for automated resets

### Frontend
- **Framework**: Vanilla JavaScript (SPA architecture)
- **Styling**: Tailwind CSS (CDN-based)
- **Icons**: Font Awesome 6.4.0
- **Typography**: Google Fonts (Poppins, Noto Nastaliq Urdu, DM Sans)
- **Reporting**: Prints and PDF generation via customized CSS media queries

---

## 3. Project Structure
```text
PRO - Hospital Management 2/
├── app.py              # Main Flask Backend (API, Auth, Logic)
├── templates/
│   └── index.html      # Massive Frontend SPA (All UI & JS logic)
├── requirements.txt    # Python dependencies
├── .env                # Environment variables (Secrets)
├── migrate_data.py     # Script for database maintenance/migration
├── render.yaml         # Render.com deployment configuration
├── vercel.json         # Vercel deployment configuration
└── DOCUMENTATION.md    # This file
```

---

## 4. Core Features

### 4.1 Authentication & User Management
- **Role-Based Access Control (RBAC)**: Supports roles like `Admin`, `Doctor`, `Psychologist`, etc.
- **Secure Login**: Rate-limited login endpoints with password hashing.
- **Forgot Password**: Time-sensitive reset links sent via email.
- **Admin Dashboard**: Create, delete, and manage user accounts and roles.

### 4.2 Patient Management
- **Admission**: Record comprehensive details including name, age, primary drug, guardian info, and 3 photos.
- **Real-time Tracking**: Monitor active vs. discharged patients.
- **Profile Prints**: Generate professional, formatted patient profiles and ID cards.

### 4.3 Financial System (Ledger Tracking)
- **Prorated Fees**: Automatic calculation of fees based on days elapsed using the formula: `(Monthly Fee / 30) * Days Elapsed`.
- **Canteen Portal**: Dedicated interface for recording daily canteen sales per patient.
- **Laundry Charges**: Automated one-time laundry fee added upon discharge.
- **Balance Due**: Dynamic calculation: `(Prorated Fee + Canteen + Laundry) - Payments Received`.
- **Payment History**: Tracks every transaction (date, amount, method).

### 4.4 Medical & Psychological Records
- **Session Notes**: Psychologists can record and track counseling sessions.
- **Medical Records**: Doctors can log medications, vitals, and general notes.
- **History View**: Searchable, chronological list of all interactions.

### 4.5 Dashboard & Metrics
- **KPI Cards**: Real-time stats on total patients, monthly admissions/discharges, and pending balances.
- **Admissions List**: Quick view of recent admissions with status indicators.

---

## 5. Database Schema (MongoDB)

| Collection | Description | Key Fields |
| :--- | :--- | :--- |
| `users` | System users and staff | `username`, `password`, `role`, `email`, `name` |
| `patients` | Patient records | `name`, `admissionDate`, `monthlyFee`, `receivedAmount`, `isDischarged`, `photo1-3` |
| `canteen_sales`| Individual sales | `patient_id`, `amount`, `item`, `date` |
| `expenses` | General ledger | `type` (incoming/outgoing), `amount`, `category`, `payment_method`, `patient_id` |
| `psych_sessions`| Counseling logs | `patient_id`, `text`, `date`, `recorded_by` |

---

## 6. Financial Logic Details

### Prorated Calculation
The system ensures fair billing by charging per day. If a patient is admitted for 12 days, they are charged exactly `12/30` of their monthly fee. 
- **Minimum Charge**: 1 day is always charged.
- **Formula**: `int((monthly_fee / 30) * max(days_elapsed, 1))`

### Automated Expenses
When a payment is added to a patient's record, the system automatically creates a entry in the `expenses` collection with `type: 'incoming'` and `auto: True` to maintain a unified financial ledger without manual double-entry.

---

## 7. Setup & Installation

### Local Development
1. **Clone the project** to your local machine.
2. **Install Python 3.8+**.
3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
4. **Configure Environment**: Create a `.env` file with:
   ```env
   MONGO_URI=mongodb://localhost:27017/hospital_management
   SECRET_KEY=your_secret_key
   GMAIL_USER=your_email@gmail.com
   GMAIL_APP_PASSWORD=your_app_password
   ```
5. **Run the application**:
   ```bash
   python app.py
   ```
6. Visit `http://127.0.0.1:5000`.

### Production Deployment
The project includes configurations for **Vercel** (`vercel.json`) and **Render** (`render.yaml`).
- Ensure environment variables are set in the provider's dashboard.
- The `gunicorn` package is used for the production-grade WSGI server.

---

## 8. Exporting Data
The system supports exporting financial and patient data using **Pandas**. You can find export endpoints under the "Reports" section which generate downloadable Excel/CSV files for audits and record-keeping.

---

## 9. Troubleshooting & Maintenance
- **MongoDB Connection**: Ensure the `MONGO_URI` is correct and accessible.
- **Email Resets**: If resets fail, check if "Less Secure Apps" (or App Passwords) are enabled for your Gmail account.
- **Photo Uploads**: Currently supports URL-based image referencing (Base64 or external links stored in MongoDB).
