flowchart TD

A[Post Sale Engine] --> B[Input: Deal Closed / Manual Entry]
B --> C[Set Timezone & Language]
C --> D[Auto Language Switch]
D --> E[Load Requirements]

E --> F1[Payment Pending Amount]
E --> F2[Document Required List]
E --> F3[Start Follow-up System]

%% Communication Channels
F1 --> G1[Call Channel 9AM-9PM]
F2 --> G2[WhatsApp 24x7]
F3 --> G3[SMS & Email Backup]

%% Payment Flow
F3 --> H{Payment Status}

H -->|Pending| I[Send Payment Reminder]
I --> J[Retry Day 0 / Day 3 / Day 5 / Day 7]
J --> F3

H -->|Partial| K[Ask Payment Proof]
K --> L[Receive Proof]
L --> M[Send to Owner for Verification]
M --> N[Owner WhatsApp: Verify Payment]

N --> O{Owner Confirm?}
O -->|Yes| P[Send Second Confirmation]
P --> Q{Confirm Again?}
Q -->|Yes| R[Update Payment]
Q -->|No| S[Cancel Update]

O -->|No| S

%% Document Flow
F3 --> T{Document Status}
T -->|Pending| U[Request Documents]
U --> V[Follow-up Day 0 / Day 2 / Day 4]
V --> F3
V --> W[Receive Document]
W --> X[Validate Document]

%% Completion
R --> Y[All Requirements Done]
X --> Y

%% Issue Resolution
Y --> Z{Issue Remaining?}
Z -->|Yes| AA[AI Try Resolve]
AA --> AB{Resolved?}
AB -->|Yes| AC[Proceed]
AB -->|No| AD[Owner Intervention]

Z -->|No| AC

%% Feedback
AC --> AE[Ask Rating 1-10]
AE --> AF{Score}

AF -->|8-10| AG[Positive]
AG --> AH[Save Testimonial]
AG --> AI[Ask Referral]

AF -->|5-7| AJ[Neutral]
AJ --> AK[Ask Improvement]
AJ --> AL[Soft Referral]

AF -->|1-4| AM[Negative]
AM --> AN[AI Resolve First]
AN --> AO{Resolved?}
AO -->|Yes| AP[Close]
AO -->|No| AQ[Owner Alert]

%% Final
AH --> AR[Update Dashboard]
AI --> AR
AK --> AR
AL --> AR
AP --> AR
AQ --> AR

AR --> AS[Morning Plan]
AR --> AT[Evening Report]
AR --> AU[Learning Loop]
AU --> E
AU --> AV[Post Sale System]