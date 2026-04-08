```mermaid
flowchart TD

A[SalesPal Marketing Engine] --> B[Business Input\n(Website / PDFs / Product Manual)]
B --> C[AI Analysis\n(USP / Audience / Industry)]
C --> D[Brand Setup\n(Tone / Style)]
D --> E[Audience Targeting + Budget Suggestion]
E --> F[AI Content Creation]

F --> G1[Images]
F --> G2[Videos / Reels]
F --> G3[Carousel]
F --> G4[Caption + CTA]
F --> G5[3D Preview System]

G5 --> H[Sample Content View]
H --> I{Action Type}

%% Social Posting
I --> J[Social Posting]
J --> K[Social Media Engine]
K --> K1[Connect Accounts\n(IG / FB / LinkedIn / X / YouTube)]
K --> K2[Post Now / Schedule]
K --> K3[Auto Publish]
K --> K4[Engagement Tracking]
K --> K5[Platform Suggestion]

%% Ads
I --> L[Run Ads]
L --> M[Ads Engine]
M --> M1[Budget Allocation]
M --> M2[Ad Launch]
M --> M3[Basic Optimization]
M --> M4[Lead Capture]

%% Campaigns
I --> N[Broadcast Campaign]
N --> O[Campaign Engine]
O --> O1[WhatsApp Broadcast]
O --> O2[SMS Campaign]
O --> O3[Email Campaign]
O --> O4[RCS Campaign]
O --> O5[Audience Selection]
O --> O6[Schedule + Timing]
O --> O7[Incoming Responses]

%% Leads
M4 --> P[Lead Management]
O7 --> P
K4 --> P

P --> P1[Lead Details\n(Name / Source / Interest)]
P --> P2[Mark Lead\n(Hot / Warm / Cold)]
P --> P3[Add Notes]
P --> P4[Set Follow-up Date]
P --> P5[View Lead History]

%% Follow-up
P --> Q[Manual Follow-up]
Q --> Q1[Call / WhatsApp]
Q --> Q2[Update Status]

Q --> R[Send to Sales Engine (Optional)]

%% Dashboard
P --> S[Dashboard]
S --> S1[Engagement Metrics]
S --> S2[Campaign Performance]
S --> S3[Lead Insights]

%% Optimization
S --> T[Digital Presence Score]
T --> U[AI Optimization Loop]
U --> F

```