# Manual test protocol: Sample Tracking web forms

This protocol is for a tester (a person or an agent) who checks the deployed app by using it as a lab member would: clicking, typing and uploading through the browser, then checking the result on the Find page and, where possible, in Notion. Follow the steps in order: later sections use records created earlier.

Each test has an ID (`T3.1`), steps and an **Expected** result. Record every test in the report (section 15).

## 0. Before you start

**The app's address.** The person who assigned the test gives you `<APP_URL>` (for example `https://<name>.up.railway.app`). If the browser asks for a username and password, ask that person for them. Never write them in the report.

**This writes to the real Notion workspace.** Records cannot be deleted through the app, only withdrawn. So:

- Every record you create starts with your prefix `<P>` = `QA-<YYYYMMDD>`, for example `QA-20261029`. If you run the protocol twice on one day, use `QA-<YYYYMMDD>b`.
- Never amend or withdraw a record that does not start with your prefix.
- Section 14 withdraws everything you created.

**Notion access.** If you can open the lab's Notion workspace, do the checks marked *In Notion*. If you cannot, mark them `NOT CHECKED`: the Find page (section 10) reads Notion live and covers most of them.

**Storage.** Open `<APP_URL>/healthz` and note `storage_backend`. The expectations about file links below are written for `local` (links like `<APP_URL>/files/...`). With `onedrive`, links point to SharePoint instead; check the file name at the end of the link the same way.

**Dialogs.** The app asks for confirmation in browser dialogs with **OK** and **Cancel**. Read the whole text and compare it with the expected text before clicking.

**Timing.** Every submission talks to Notion; allow 5 to 30 seconds. Do not click a submit button twice.

**Console errors that are expected.** When a name already exists, the browser console shows `409 (CONFLICT)` for `/api/validate-sample` or `/api/validate-result`. That is how the app detects an amendment, not a bug.

## 1. Prepare test files

Create these on the machine running the browser:

| File | Content |
|---|---|
| `qa-scan.csv` | two lines: `a,b` and `1,2` |
| `qa-scan-v2.csv` | two lines: `a,b` and `1,3` |
| `record.json` | `{"instrument": "QA"}` |
| `qa-photo.png` | any small picture (under 1 MB) |
| `A/image.jpg` and `B/image.jpg` | two **different** small pictures with the **same** file name, in two folders |
| `qa-bad.exe` | any content |

## 2. Names used below

| Placeholder | Record |
|---|---|
| `<P>-S1` | root sample, composition `Ba(NO3)2`, with photos and a new Source |
| `<P>-S2` | root sample, composition `Co3O4`, submitted right after S1 without reloading |
| `<P>-S3` | sub-sample of S1 |
| `<P>-C1` … `<P>-C6` | composition cases |
| `<P>-D1` | data entry with a link |
| `<P>-D2` | data entry with an uploaded file |
| `<P>-D3` | data entry whose file is named `record.json` |
| `<P>-D4` | data entry derived from D1 (parent dataset) |
| `QA Tester <P>` | a new Source (person) |

## 3. Start page

**T3.1 Layout.** Open `<APP_URL>/`.
**Expected:** the dark header reads "Center for Advanced Materials & Manufacturing" and "Sample Management System" over an orange stripe; the card is titled "Sample Tracking" and has these sections:

- "Add new records": **+ Add sample**, **+ Add results**
- "Existing records": **Find a record**, **Withdraw a record**, and the hint "To correct a sample or data entry, submit its name again in its form; you see what would change before anything is saved."
- "Browse in Notion": **Samples ↗**, **Results ↗**, **Sources ↗** (only if configured), and possibly "Open the Notion workspace ↗"
- No CSV download buttons anywhere.

**T3.2 Navigation.** Click each button, check the page, then click "← Home".
**Expected:** + Add sample → "Sample Submission Form"; + Add results → "Add Data Entry"; Find a record → "Find a record"; Withdraw a record → "Withdraw a record". Every page has "← Home", which returns to the start page. The Notion buttons open Notion in a new tab.

**T3.3 Health check.** Open `<APP_URL>/healthz`.
**Expected:** JSON with `"status": "ok"` and `storage_backend`.

## 4. Sample form: required fields

Open **+ Add sample**.

**T4.1 Nothing filled.** Click **✓ Submit to Database**.
**Expected:** "Please fill all required fields before submitting." Nothing is created.

**T4.2 Root sample without composition.** Name `<P>-X1`, 2. Sample Type **Root Sample**, Composition empty, submit.
**Expected:** the same message; the browser may also highlight Composition. Nothing is created.

**T4.3 Sub-sample without parent.** Name `<P>-X1`, Sample Type **Sub-Sample**, leave "2.1. Parent Sample" on "-- Select Parent Sample --", submit.
**Expected:** refused with the same message. Nothing is created.

## 5. Creating samples

**T5.1 Sample with photos and a new Source.** Reload the form, then fill in:

1. Sample Name: `<P>-S1`
2. Sample Type: **Root Sample**
3. 2.1. Composition: `Ba(NO3)2`
4. 2.2. Synthesis: any option (for example **Other**)
5. 2.3. Synthesis Details: `QA synthesis at 600 °C, 5 µm film`
6. 2.4. Source: click **+ Add Source**; Name and Surname `QA Tester <P>`, Email `qa@example.org`, Affiliation `QA Lab`; click **Save Source**. The person appears in the list.
7. 3. Processing: any option (for example **Annealing**)
8. 5. Status: **In Work**
9. 6. Photos: **+ Add Files**, add `qa-photo.png`, `A/image.jpg` and `B/image.jpg` (three files)
10. Click **✓ Submit to Database**.

**Expected:** a green message "Sample submitted successfully" with "The record was saved. You can download a PDF receipt for your records." and **Download PDF receipt**. The form is cleared.

**T5.2 Receipt.** Click **Download PDF receipt**.
**Expected:** a PDF titled "Sample Submission - `<P>-S1`" with the values you entered and a QR code ("Scan to open in Notion"). The receipt shows the composition as typed (`Ba(NO3)2`).

**T5.3 A second sample from the same open page.** Do **not** reload. Fill in Name `<P>-S2`, Root Sample, Composition `Co3O4`, no photos; submit.
**Expected:** "Sample submitted successfully". **Fail if** the message is about loaded JSON or "already processed".

**T5.4 Sub-sample.** Sample Type **Sub-Sample**; in "2.1. Parent Sample" choose `<P>-S1`; Name `<P>-S3`; submit.
**Expected:** "Sample submitted successfully".

**T5.5 In Notion** (Samples database), open `<P>-S1`.
**Expected:** Composition `BaN2O6`; Elements Ba, N, O; User composition names `Ba(NO3)2`; Status in work; Source `QA Tester <P>`; Archive Status "Archive complete". Photos holds **three** files: `qa-photo.png`, `image.jpg` and `image-1.jpg`, so the second `image.jpg` was renamed, not lost. QRCode holds `<P>-S1_qr.png`. `<P>-S3` has Parent Sample `<P>-S1` and Composition `BaN2O6`.

## 6. Compositions

Create one root sample per row (any Synthesis; no photos). For C2, if you cannot type `·`, type `CuSO4*5H2O`.

**T6.1 Valid formulas.**

| Name | Type in Composition | **Expected** Composition | Elements | User composition names |
|---|---|---|---|---|
| `<P>-C1` | `Al0.7Sc0.3N` | `Sc3Al7N10` | Sc, Al, N | `Al0.7Sc0.3N` |
| `<P>-C2` | `CuSO4·5H2O` | `CuH10SO9` | Cu, H, S, O | the typed text |
| `<P>-C3` | `Na` | `Na` | Na | empty |
| `<P>-C4` | `PbTiO3` | `TiPbO3` | Ti, Pb, O | `PbTiO3` |

Each submission should show "Sample submitted successfully". Check the values on the Find page (section 10) or in Notion.

**T6.2 Invalid formulas.**

| Name | Type in Composition | **Expected** message |
|---|---|---|
| `<P>-C5` | `XyZ` | "✗ Xy is not a chemical element (in XyZ)." |
| `<P>-C6` | `N/A` | "✗ Composition is required" |

**Expected:** neither record appears in the Find page's sample list.

## 7. Sample amendments

Submitting the name of an existing sample amends it. You must fill in the required fields again (Name, Sample Type, Composition).

**T7.1 No change.** Name `<P>-S2`, Root Sample, Composition `Co3O4`; submit.
**Expected:** a dialog "A sample named "`<P>-S2`" already exists." listing "- No field changes detected". Click OK. **Expected:** "No changes detected. Existing sample left unchanged."

**T7.2 Cancel.** As T7.1, plus 2.3. Synthesis Details `QA amended details`; submit.
**Expected:** the dialog lists "- Synthesis Details". Click **Cancel**. **Expected:** "A sample named "`<P>-S2`" already exists. Please change the sample name." On the Find page, `<P>-S2` has no Synthesis Details.

**T7.3 Amend.** Repeat T7.2 and click **OK**.
**Expected:** "Sample amended successfully". The Find page shows Synthesis Details `QA amended details`, and Composition is still `Co3O4`.

**T7.4 Same composition, other notation.** Name `<P>-S1`, Root Sample, Composition `BaN2O6`, everything else empty; submit.
**Expected:** the dialog says "- No field changes detected": `BaN2O6` and `Ba(NO3)2` are the same composition. Click Cancel.

**T7.5 Empty fields keep their values.** Name `<P>-S1`, Root Sample, Composition `Ba(NO3)2`, 4. Processing Details `QA processing`; leave Synthesis, Processing and Status empty; submit.
**Expected:** the dialog lists only "- Processing Details". Click OK, then "Sample amended successfully". On the Find page, Synthesis, Processing and Status keep their earlier values.

**T7.6 Correcting a Source's details.** Name `<P>-S2`, Root Sample, Composition `Co3O4`; **+ Add Source** with Name and Surname `QA Tester <P>` (exactly as before), Affiliation `QA Lab 2`; **Save Source**; submit.
**Expected:**

1. The amendment dialog lists "- Source". Click OK.
2. A second dialog: "These Sources already exist with other details:" and "- QA Tester `<P>`: Affiliation "QA Lab" → "QA Lab 2"". Click **Cancel**.

**Expected:** "Nothing was saved. Correct or remove the Source details, or submit again and confirm the change." Submit again and click OK in both dialogs. **Expected:** "Sample amended successfully". *In Notion*, the People page `QA Tester <P>` has Affiliation `QA Lab 2`.

## 8. Data entry form: required fields

Open **+ Add results**. The checks run in this order:

**T8.1** Click **✓ Submit** with nothing filled. **Expected:** "✗ Parent Entry is required."

**T8.2** 4. Parent Entry **sample**, no Parent Sample. **Expected:** "✗ Parent Sample is required when Parent Entry is set to sample."

**T8.3** Also choose 4.1 Parent Sample `<P>-S1`, but no Entry type. **Expected:** "✗ Entry type is required."

**T8.4** Also 1. Name `<P>-X2` and 2. Entry type (any), but 6. Data left at "-- Select --". **Expected:** "✗ Please fill required fields: Data upload method is required."

**T8.5** 6. Data **Link** with an empty link. **Expected:** "✗ Link is required."

**T8.6** 6. Data **File** without choosing a file. **Expected:** "✗ File is required."

## 9. Creating data entries

**T9.1 Link entry.** Reload the form, then fill in:

- Name `<P>-D1`
- Entry type: any (for example **dataset**)
- 3. Brief Description `QA link entry`
- Parent Entry **sample**, Parent Sample `<P>-S1`
- 5. Characterisation: pick one (for example **XRD**)
- 6. Data **Link**, Link `https://example.org/qa-d1`

Submit. **Expected:** "Data submitted successfully" with **Download PDF receipt**. The form is cleared.

**T9.2 File entry, same open page.** Do **not** reload. Name `<P>-D2`, Entry type, Parent sample `<P>-S1`, Data **File**, choose `qa-scan.csv`; submit.
**Expected:** "Upload completed ✔", then "Data submitted successfully". **Fail if** you see "already processed" or "already used to submit".

**T9.3 A file named record.json.** Name `<P>-D3`, Parent sample `<P>-S1`, Data **File**, choose `record.json`; submit.
**Expected:** success. On the Find page, `<P>-D3`'s Link ends in `/record-1.json`. Opening the link downloads `{"instrument": "QA"}`, so the app's own record did not overwrite your file.

**T9.4 File type not allowed.** Name `<P>-X3`, Parent sample `<P>-S1`, Data **File**, `qa-bad.exe`; submit.
**Expected:** the upload status shows "Upload failed: File type is not allowed: .exe" and the message reads "✗ Upload failed. Fix the issue and try Submit again." `<P>-X3` is not created.

**T9.5 Entry derived from another entry.** Name `<P>-D4`, Parent Entry **dataset**, 4.1 Parent Dataset `<P>-D1`, Data **Link** `https://example.org/qa-d4`; submit.
**Expected:** success. On the Find page, `<P>-D4` has Related Results `<P>-D1` and Sample `<P>-S1`.

**T9.6 Download a stored file.** On the Find page open `<P>-D2` and click its Link.
**Expected:** `qa-scan.csv` downloads, with content `a,b` / `1,2`.

## 10. Find a record

**T10.1 Lists.** Open **Find a record**.
**Expected:** "1. What are you looking for? \*" set to **A sample**; "2. Which one? \*" lists your `<P>-…` samples; the help line reads like "12 samples." (or "…, 1 withdrawn."). There is no free-text search box.

**T10.2 A sample.** Choose `<P>-S1`.
**Expected:** a box headed `<P>-S1` with a grey "in work" badge, and rows that include:

- Sample Type `Root Sample`, Status `in work`, Composition `BaN2O6`, Elements `Ba, N, O`
- Synthesis Details `QA synthesis at 600 °C, 5 µm film` (° and µ shown correctly)
- Processing Details `QA processing`, Source `QA Tester <P>`, User composition names `Ba(NO3)2`
- Archive Status `Archive complete`
- **Created** and **Last edited** as dates (YYYY-MM-DD)

It also lists "Data entries (4)" (D1, D2, D3, D4) and "Sub-samples (1)" (`<P>-S3`).
**Fail if** you see QRCode, Photos, Created by, Last edited by or Submission ID rows.

**T10.3 Buttons.** **Expected:** **Download PDF** (orange, white text), **Download JSON** (dark grey, **white text, readable**), **Open in Notion ↗** (white, grey text).

**T10.4 PDF.** Click **Download PDF**.
**Expected:**

- File `<P>-S1.pdf`; header "Sample - `<P>-S1`" and "Sample record"; a QR code with "Scan to open in Notion"
- Summary cards: Name, Type `Root Sample`, Status `in work`
- A "Record Details" section with the rows above, then the linked data entries and sub-samples
- A "Photos" section with your photos (up to four); note in the report whether they appear
- The word "receipt" appears nowhere.

**T10.5 JSON.** Click **Download JSON**.
**Expected:** file `<P>-S1.json` with keys `kind` (`sample`), `name`, `status`, `withdrawn` (`false`), `notion_page_id`, `notion_url`, `created_time`, `last_edited_time`, `fields`, `linked` and `exported_at`. `fields.Elements` is a list `["Ba", "N", "O"]`, and the text shows `600 °C, 5 µm` (not `°`).

**T10.6 Notion link.** Click **Open in Notion ↗**. **Expected:** `<P>-S1` opens in Notion in a new tab.

**T10.7 A data entry.** Switch to **A data entry**, choose `<P>-D2`.
**Expected:** Data Type, Parent Entry `sample`, Sample `<P>-S1`, Upload Method `file`, and Link as a clickable address ending in `qa-scan.csv`.

## 11. Data entry amendments

Submitting the name of an existing data entry amends it. Fill in the required fields again (Name, Entry type, Parent Entry, Parent Sample).

**T11.1 Data left empty.** Name `<P>-D1`, Entry type, Parent sample `<P>-S1`, Brief Description `QA link entry v2`, 6. Data left at "-- Select --"; submit.
**Expected:** a dialog "A data entry named "`<P>-D1`" already exists." listing only "- Brief Description", ending "Fields you left empty keep their current values, and earlier files and records are kept." Click OK. **Expected:** "Data entry amended successfully". On the Find page the description is `QA link entry v2`, and Link is still `https://example.org/qa-d1`.

**T11.2 Cancel.** As T11.1 with description `QA cancelled`; click **Cancel**.
**Expected:** "A data entry named "`<P>-D1`" already exists. Change the name to create a new entry." Nothing changes.

**T11.3 A new file.** Name `<P>-D2`, Entry type, Parent sample `<P>-S1`, Data **File**, `qa-scan-v2.csv`; submit.
**Expected:** the dialog lists "- Link (to the new file; the current file is kept)". Click OK, then "Data entry amended successfully". On the Find page, Link ends in `qa-scan-v2.csv` and downloads `a,b` / `1,3`. The old link from T9.6 still downloads `a,b` / `1,2`.

**T11.4 The parent cannot change.** Name `<P>-D1`, Entry type, Parent sample `<P>-S2`, description `QA move`; submit.
**Expected:** "✗ The data entry '`<P>-D1`' already exists under sample '`<P>-S1`'. The parent of an existing entry can't change. To file this data under another parent, use a new name." Nothing changes.

## 12. Withdraw a record

**T12.1 Lists.** Open **Withdraw a record**.
**Expected:** "1. What do you want to withdraw? \*" (A sample / A data entry), "2. Which one? \*" with the hint "Only active records are listed.", and "3. Reason \*". The **Withdraw** button stays disabled until a record is chosen.

**T12.2 Preview.** Choose `<P>-S1`.
**Expected:** a box with Sample Type, Composition `BaN2O6`, Status, Created, "Data entries (4), which stay active" (D1 to D4), "Sub-samples (1), which stay active" (`<P>-S3`), and "Open in Notion ↗". Do **not** withdraw S1 now.

**T12.3 A reason is required.** Choose `<P>-S2`, leave Reason empty, click **Withdraw**.
**Expected:** "Please give a reason for withdrawing this record." Nothing changes.

**T12.4 Withdraw a sample.** `<P>-S2`, Reason `QA test withdrawal`, click **Withdraw**.
**Expected:** a dialog 'Withdraw the sample "`<P>-S2`"?' explaining that it will be hidden from Notion's main views and nothing is deleted. Click OK. **Expected:** the green message "The sample '`<P>-S2`' is withdrawn and hidden from Notion's main views. Nothing was deleted." with **Download PDF receipt**; its PDF is titled "Withdrawal - `<P>-S2`" and shows the reason and "Previous Status". `<P>-S2` drops out of the list.

**T12.5 Effects.**
**Expected:**

- Find: `<P>-S2` is listed as "`<P>-S2` (withdrawn)"; it shows a red "withdrawn" badge, the note "Withdrawn: QA test withdrawal. Submitting its name again in its form makes it active again." and a Withdrawn Reason row.
- The sample form (Sub-Sample → Parent Sample) and the data-entry form (Parent Sample) no longer offer `<P>-S2`.
- *In Notion*, Samples → **Table** no longer shows `<P>-S2`, and Samples → **Withdrawn** lists it with its reason.

**T12.6 Withdraw a data entry.** Choose **A data entry**, `<P>-D3`, Reason `QA test withdrawal`, Withdraw, OK.
**Expected:** "The data entry '`<P>-D3`' is withdrawn and hidden from Notion's main views. Nothing was deleted." In Find, `<P>-S1`'s "Data entries" list shows "`<P>-D3` (withdrawn)". On the Withdraw page, `<P>-S1`'s preview lists only D1, D2 and D4.

## 13. Making a withdrawn record active again

**T13.1 Sample.** Sample form: Name `<P>-S2`, Root Sample, Composition `Co3O4`, Status empty; submit.
**Expected:** the dialog **starts** with ""`<P>-S2`" is withdrawn. Amending it makes it active again." and lists "- Status" and "- Withdrawn Reason". Click OK, then "Sample amended successfully".
**Expected:** Find shows Status `in work` with no Withdrawn Reason row; the Withdraw page lists `<P>-S2` again; *in Notion* it is back in Samples → Table.

**T13.2 Data entry.** Data-entry form: Name `<P>-D3`, Entry type, Parent sample `<P>-S1`, Data empty, Brief Description `QA reinstated`; submit.
**Expected:** the dialog starts with ""`<P>-D3`" is withdrawn. Amending it makes it active again." Click OK, then "Data entry amended successfully". Find shows Status `active`.

**T13.3 Nothing new under a withdrawn sample.** Withdraw `<P>-C3` (Reason `QA`). Open the data-entry form and the Sub-Sample parent list.
**Expected:** `<P>-C3` is offered in neither Parent Sample list.

## 14. Phone layout

**T14.1** Set the browser to 375 × 812 (phone) and open the start page, both forms, Find and Withdraw.
**Expected:** no sideways scrolling; buttons stack and span the width; all fields can be reached and filled; on Find, the three download buttons are full width and readable.

## 15. Clean up

**T15.1** On the Withdraw page, withdraw every record of yours that is still active, with Reason `QA test record, cleanup`:

- samples: S1, S2, S3, C1, C2, C3 (if still active), C4
- data entries: D1, D2, D3, D4

**Expected:** the Withdraw lists contain no `<P>-…` record. *In Notion*, Samples → Table and Results → All Results show none of them; the **Withdrawn** views list them all.

The Source `QA Tester <P>` stays in People: people cannot be withdrawn. Mention it in the report so it can be removed by hand if wanted.

## 16. Not bugs (known behaviour)

- A withdrawn record is hidden only from Notion's main views. It still opens from a direct link, Notion search, its QR code, or the relation list of a linked record.
- Data entries and sub-samples stay active when their sample is withdrawn (T12.2 lists them).
- A data entry or sub-sample whose parent is withdrawn cannot be amended until the parent is active again: the forms list only active parents.
- Molecular formulas are reduced like any other (`C6H6` becomes `HC`); the typed form is kept in User composition names. Isotope symbols such as `D2O` are refused.
- The record-sheet PDF fetches only photos stored by Notion. A photo missing from the PDF is a finding to report, with the sample's name, not a failure of the rest of T10.4.
- `409 (CONFLICT)` in the console during duplicate checks (section 0).

## 17. Report

Return one table and the details of every failure.

| ID | Result | Notes |
|---|---|---|
| T3.1 | PASS / FAIL / BLOCKED / NOT CHECKED | |
| … | | |

For each FAIL give:

- **Steps:** exactly what you did, including names and files
- **Expected:** quoted from this protocol
- **Actual:** the exact message or behaviour, quoted
- **Evidence:** a screenshot; the browser console errors if any (other than the expected 409s); the time (UTC)
- **Records involved:** names starting with `<P>`

Finish with:

- the list of records you created (for the cleanup check)
- whether `storage_backend` was `local` or `onedrive`
- which checks you could not do (for example, no Notion access)
