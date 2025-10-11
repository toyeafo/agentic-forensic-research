# CYFI 700 - HTC Desire Forensic Analysis

## Question 1: User Account Information

**Email Account:** cfttmobile1@gmail.com
**Telephone Number:** 8887771212
**Profile Picture:** profile.jpg

**Evidence Location:**
- Email and phone number found in `contacts2.db` located in partition `HTC_Desire_626N.img63` at offset `7389184`, within the `com.android.providers.contacts` package data.
- Profile picture extracted from the same database.

## Question 2: ICCID Number

**ICCID:** 8901260472997564858

**Evidence Location:**
- Found in the `siminfo` table of `telephony.db`, located in partition `HTC_Desire_626N.img63` at offset `7389184`, within the `com.android.providers.telephony` package data.

## Question 3: Geographic Area of Area Code

The area code `888` is a **non-geographic, toll-free area code** used across North America. It is not registered to a specific city or region.

## Question 4: Images of Persons

Three images of persons have been extracted:
- `emma-girl.jpg`
- `IMG_20180215_123931_511.jpg`
- `IMAG0001.jpg`

**Evidence Location:**
- All images were found in partition `HTC_Desire_626N.img63` at offset `7389184`.

## Question 5: Memo Excerpt

Here is an excerpt from the long memo found on the device:

> The goal of the CFTT project at NIST is to establish a methodology for testing computer forensic software tools by development of general tool specifications, test procedures, test criteria, test sets, and test hardware. The results provide the information necessary for toolmakers to improve tools, for users to make informed choices about acquiring and using computer forensics tools, and for interested parties to understand the tools capabilities.

**Evidence Location:**
- The full memo text was found in the `notes` table of `htcnotes.db`, located in partition `HTC_Desire_626N.img63` at offset `7389184`.

## Question 6: Active Images (JPG and GIF)

Two active images have been extracted:
- `IMAG0001.jpg`
- `homer.gif`

**Evidence Location:**
- Both images were found in partition `HTC_Desire_626N.img63` at offset `7389184`.