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

## Question 7: Deleted Audio File

**File Type:** ISO Media, MP4 v2 [ISO 14496-14]

**Evidence Location:**
- The deleted file, `Hinder.mp4`, was found in partition `HTC_Desire_626N.img63` at offset `7389184`.

## Question 8: Deleted Image File

**Image Format:** PC bitmap, Windows 3.x

**Evidence Location:**
- The deleted file, `winter.bmp`, was found in partition `HTC_Desire_626N.img63` at offset `7389184`.

## Question 9: John Doe's Social Media Profile Picture

**Profile Picture:** `john_doe.jpg`

**Evidence Location:**
- The profile picture was found in the Twitter cache at `media/0/Android/data/com.twitter.android/cache/users/1bd816eb801ebe83955c29cb4871afee.0` in partition `HTC_Desire_626N.img63` at offset `7389184`.

## Question 10: Volatile vs. Non-Volatile Memory

**Answer:**
Passwords, encryption keys, usernames, and app data can be found in **non-volatile (NAND flash) memory**. This is because this type of memory is persistent and stores data even when the device is powered off. Volatile memory (RAM) is temporary and is cleared when the device loses power.

**Exhibit:**
The file `misc/keychain/pins` contains a list of domains and their associated certificate pins. This is an example of sensitive security data stored in non-volatile memory. Here is an excerpt:

```
*.google.com=false|26a33255b652f62b17b0b0535132d5baa8226a9dc151f369dee4a061cb3be0460fc9b9d827d02031fb0135f07ab5cf7b966c8c332b763c3e2c9904e78311f8e2,093d12744be4f166b03035e8c012296f9cfba246dd18e3754c54957001d751b68c5f0d4f28adaa915329feea72863da855b566a457a33bfa55857a10e8fca9a3,3648152365ee87275bc767f5994233a37fee1debeee4b5fbe5fecee58ce6219ea8a8241f80464a4aec1790035631f6c1681c7d1e122b175f611dab965093164a,9122ae58ee7d157d9656eda83539d1dc1a8a0fbb68ee0808373fdb53b9895e30180e769af6b38b0c8028cbe545de3d8697d4e7ff1aae08c27bfdbba9866b8b04,7c60c2209b80b1b060d32c02d089de84798106d1f5c5745ca58aa3d82ad3abd6e8b42c0ab02476ad0665ffdb93ed6535e8871561b49a70da3064e39cd5e3e1eb,34c8a0dd00c56f3184463d8ce39e04ca05fce99768e93b2051a436498a8bcea0e752ae7c6adf567ca46b2483762d15fa4c77bef0b0f1c178ddc2638a3164ea72,82cb8efb37a1e084693f6d8cf8b35f0082aaf61676a7c8bf37ec06396037168a635278a1a8fa5c138cd79eb6bfab434ffb1133cf765eea2de2df478148903ae0
```