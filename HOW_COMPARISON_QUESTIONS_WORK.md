# How the System Handles Comparison Questions

## Quick Answer

**Question:** "Compare Biosphere 2 results with Amazon rainforest and Brazilian rainforest"

**Current Response:**
The system will:
1. ✅ Provide Biosphere 2 data (temperature, humidity, etc.) from the database
2. ✅ Clearly state that Amazon/Brazilian rainforest data is not available in this database
3. ✅ Explain that the system is designed for Biosphere 2 data only
4. ✅ Offer to help with detailed Biosphere 2 analysis instead
5. ❌ Will NOT make up or hallucinate external data

---

## Example Response

**User asks:** "How does the temperature and humidity in Biosphere 2 compare to the Amazon rainforest and Brazilian rainforest?"

**System responds:**
```
"Based on the Biosphere 2 sensor data, the rainforest area maintains temperatures 
ranging from 61.4°F to 94.5°F, averaging 76.1°F, with humidity levels between 65% 
and 95% during the monitoring period (September 9 - October 9, 2025). 

However, I don't have access to sensor data from the Amazon rainforest or Brazilian 
rainforest in this database to make a direct comparison. This system is specifically 
designed to analyze Biosphere 2 environmental monitoring data.

I can help you explore the Biosphere 2 environmental patterns in more detail, including 
temperature trends, humidity variations, and correlations between different sensor 
systems. Would that be helpful?"
```

---

## What Changed

### **Before (Problem):**
- Prompt told AI to "search harder" even when data doesn't exist
- Could lead to hallucination (making up Amazon/Brazilian data)
- Could give confusing partial answers

### **After (Fixed):**
- Added specific instructions for comparison questions
- AI now acknowledges what it can and cannot do
- Provides Biosphere 2 data honestly
- Clearly states limitations
- Offers helpful alternatives

---

## Key Features

1. **Honest:** Never makes up external data
2. **Helpful:** Provides what it can (Biosphere 2 data)
3. **Clear:** States limitations explicitly
4. **Positive:** Frames as opportunity to help with Biosphere 2 analysis
5. **Transparent:** Explains system scope

---

## Detection

The system detects comparison questions by looking for keywords like:
- "compare", "comparison", "versus", "vs"
- "amazon", "brazilian", "brazil"
- "external", "other", "different"

When detected, the prompt includes special comparison instructions.

---

## Future Enhancement

To actually support comparisons, you would need to:
1. Add external data sources (Amazon rainforest sensor data, etc.)
2. Create document chunks for external data
3. Tag chunks with location metadata
4. Enhance search for multi-location queries

But for now, the system handles these questions honestly and helpfully.

---

**See `COMPARISON_QUESTION_HANDLING.md` for detailed technical documentation.**


