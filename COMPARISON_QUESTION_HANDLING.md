# How the System Handles Comparison Questions with External Data

## Current Behavior

### **Question Example:**
"Compare the temperature and humidity in Biosphere 2 with the Amazon rainforest and Brazilian rainforest."

### **What Happens:**

1. **RAG Search:** The system searches for "amazon rainforest", "brazilian rainforest" in the database
2. **Result:** Finds nothing (database only contains Biosphere 2 sensor data)
3. **Context Retrieved:** Only Biosphere 2 data is retrieved
4. **AI Prompt:** Current prompt forbids saying "data does not contain" and tells AI to "search harder"
5. **Likely Response:** AI might:
   - Give only Biosphere 2 data without mentioning the comparison
   - Try to infer or make up Amazon/Brazilian data (hallucination risk)
   - Give a confusing partial answer

### **Problem:**
The current prompt doesn't have instructions for handling legitimate external data requests that don't exist in the database.

---

## Current Prompt Limitations

The current prompt says:
```
7. ABSOLUTELY FORBIDDEN: Never say "unfortunately", "data does not contain", 
   "without direct measurements", "I cannot", "not enough information" - 
   if you can't find it in the first search, search again in the DATA AVAILABLE section
```

This works well for data that SHOULD exist (like humidity data that's in the database), but fails for legitimate external comparisons.

---

## Improved Prompt Section for Comparisons

Add this section to handle external comparison questions:

```python
COMPARISON QUESTIONS WITH EXTERNAL DATA:
- If the question asks to compare Biosphere 2 with external locations (Amazon rainforest, 
  Brazilian rainforest, other ecosystems, etc.):
  1. FIRST: Provide the Biosphere 2 data from DATA AVAILABLE section
  2. THEN: Clearly state that external comparison data is not available in this database
  3. OFFER: Provide what you can from Biosphere 2 data and explain the limitation
  4. FORMAT: "Based on the Biosphere 2 sensor data, [provide Biosphere 2 metrics]. 
     However, I don't have access to sensor data from [external location] in this database 
     to make a direct comparison. I can help you analyze the Biosphere 2 environmental 
     conditions in detail if that would be helpful."

EXAMPLES OF GOOD COMPARISON ANSWERS:
✓ "Based on the Biosphere 2 sensor data, the rainforest area maintains temperatures 
   ranging from 61.4°F to 94.5°F, averaging 76.1°F, with humidity levels between 
   65% and 95%. However, I don't have access to sensor data from the Amazon or 
   Brazilian rainforests in this database to make a direct comparison. I can help 
   you explore the Biosphere 2 environmental patterns in more detail if that would 
   be helpful."

✓ "The Biosphere 2 rainforest shows [specific metrics from data]. For comparison 
   with natural rainforests like the Amazon or Brazilian rainforest, I'd need access 
   to their sensor data, which isn't available in this system. Would you like me to 
   provide more detailed analysis of the Biosphere 2 conditions?"

EXAMPLES OF BAD COMPARISON ANSWERS:
✗ Making up Amazon/Brazilian data that doesn't exist
✗ Only giving Biosphere 2 data without acknowledging the comparison request
✗ Saying "I cannot" (too negative) - instead say "I don't have access to..."
✗ Ignoring the comparison part of the question entirely
```

---

## Recommended Response Format

### **Good Response Structure:**

1. **Acknowledge the request:** "You're asking to compare Biosphere 2 with [external location]..."
2. **Provide Biosphere 2 data:** Give specific metrics from the database
3. **State limitation clearly:** "I don't have access to [external] data in this database"
4. **Offer alternatives:** "I can help you analyze Biosphere 2 conditions in detail"

### **Example Response:**

```
"Based on the Biosphere 2 sensor data, the rainforest area maintains temperatures 
ranging from 61.4°F to 94.5°F, averaging 76.1°F, with humidity levels between 65% 
and 95% during the monitoring period (September 9 - October 9, 2025). 

However, I don't have access to sensor data from the Amazon rainforest or Brazilian 
rainforest in this database to make a direct comparison. This system is specifically 
designed to analyze Biosphere 2 environmental monitoring data.

I can help you explore the Biosphere 2 environmental patterns in more detail, 
including temperature trends, humidity variations, and correlations between different 
sensor systems. Would that be helpful?"
```

---

## Updated Prompt Code

Here's the improved prompt section to add to `spectacular_rag_web_app.py`:

```python
COMPARISON QUESTIONS WITH EXTERNAL DATA:
- If the question asks to compare Biosphere 2 with external locations (Amazon rainforest, 
  Brazilian rainforest, other ecosystems, cities, etc.):
  1. FIRST: Provide the Biosphere 2 data from DATA AVAILABLE section with specific metrics
  2. THEN: Clearly state: "I don't have access to [external location] sensor data 
     in this database to make a direct comparison."
  3. EXPLAIN: "This system is specifically designed to analyze Biosphere 2 environmental 
     monitoring data."
  4. OFFER: "I can help you explore the Biosphere 2 environmental patterns in more 
     detail, including [specific capabilities]."
  5. FORMAT: Be honest and helpful - provide what you can, acknowledge what you can't

EXAMPLES OF GOOD COMPARISON ANSWERS:
✓ "Based on the Biosphere 2 sensor data, the rainforest area maintains temperatures 
   ranging from 61.4°F to 94.5°F, averaging 76.1°F, with humidity levels between 
   65% and 95%. However, I don't have access to sensor data from the Amazon or 
   Brazilian rainforests in this database to make a direct comparison. This system 
   is specifically designed to analyze Biosphere 2 environmental monitoring data. 
   I can help you explore the Biosphere 2 environmental patterns in more detail if 
   that would be helpful."

✓ "The Biosphere 2 rainforest shows [specific metrics from data]. For comparison 
   with natural rainforests like the Amazon or Brazilian rainforest, I'd need access 
   to their sensor data, which isn't available in this system. Would you like me to 
   provide more detailed analysis of the Biosphere 2 conditions?"

EXAMPLES OF BAD COMPARISON ANSWERS:
✗ Making up Amazon/Brazilian data that doesn't exist (hallucination)
✗ Only giving Biosphere 2 data without acknowledging the comparison request
✗ Saying "I cannot" or "unfortunately" (too negative)
✗ Ignoring the comparison part of the question entirely
✗ Providing general knowledge about Amazon without stating it's not from the database
```

---

## Implementation

### **Option 1: Add to Existing Prompt (Recommended)**

Add the comparison section to the existing prompt in `spectacular_rag_web_app.py` around line 1185 (after SENSOR TYPES QUESTIONS section).

### **Option 2: Detect Comparison Questions**

Add detection logic before building the prompt:

```python
# Detect comparison questions
is_comparison = any(keyword in question_lower for keyword in [
    'compare', 'comparison', 'versus', 'vs', 'vs.', 'difference between',
    'amazon', 'brazilian', 'brazil', 'external', 'other'
])

if is_comparison:
    # Add comparison-specific instructions to prompt
    prompt += "\n\nCOMPARISON QUESTIONS: [instructions]"
```

---

## Testing Examples

### **Test Question 1:**
"Compare the temperature in Biosphere 2 with the Amazon rainforest"

**Expected Response:**
- Provides Biosphere 2 temperature data
- States Amazon data not available
- Offers to help with Biosphere 2 analysis

### **Test Question 2:**
"How does Biosphere 2 humidity compare to Brazilian rainforest?"

**Expected Response:**
- Provides Biosphere 2 humidity data
- States Brazilian rainforest data not available
- Acknowledges the comparison request

### **Test Question 3:**
"What's the difference between Biosphere 2 and natural rainforests?"

**Expected Response:**
- Provides Biosphere 2 metrics
- Explains limitation clearly
- Offers detailed Biosphere 2 analysis

---

## Key Principles

1. **Honesty:** Never make up external data
2. **Helpfulness:** Provide what you can (Biosphere 2 data)
3. **Clarity:** Clearly state what's not available
4. **Positivity:** Frame limitations as opportunities ("I can help with...")
5. **Transparency:** Explain the system's scope

---

## Future Enhancement

To actually support comparisons, you would need to:
1. Add external data sources (Amazon rainforest sensor data, etc.)
2. Create separate document chunks for external data
3. Tag chunks with location metadata
4. Enhance search to handle multi-location queries
5. Create comparison visualization capabilities

But for now, the improved prompt ensures honest, helpful responses when external data isn't available.


