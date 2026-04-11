# Recommendation Engine

**Created:** 2026-04-10
**Status:** Not Started
**Workspace:** https://fevm-serverless-stable-1dpktm.cloud.databricks.com
**Catalog:** serverless_stable_1dpktm_catalog
**Owner:** Ben MacKenzie

---

## Problem Statement

ACME has a catalog of 1,000 courses and wants to send personalized course recommendation emails to their customer base (accountants). Rather than building a real-time RAG agent, the approach demonstrates a **batch inference pattern**: pull relevant context (course descriptions via vector search + customer segmentation data) into a single prompt and use `ai_query()` to generate recommendations at scale. This is a more practical, cost-effective pattern for periodic batch use cases like email campaigns.

## Technical Approach

The demo uses a batch pipeline pattern:
1. **Course descriptions** stored in a Delta table with a **Vector Search index** for semantic retrieval
2. **Customer segmentation data** stored in a Delta table (accountant profiles, specializations, experience level, interests)
3. **Batch enrichment pipeline** that joins customer context with relevant courses (via vector similarity) into a single wide table
4. **`ai_query()` call** on the enriched table to generate personalized course recommendations per customer
5. **Output table** with customer email + recommended courses + generated email copy

### Components
| Component | Description | Status |
|-----------|-------------|--------|
| Course data & Vector Index | Delta table of 1,000 courses + Mosaic AI Vector Search index on descriptions | Not Started |
| Customer segmentation table | Delta table with accountant profiles, segments, and preferences | Not Started |
| Batch enrichment notebook | Pipeline that retrieves relevant courses per customer via vector search and assembles full context | Not Started |
| ai_query recommendation notebook | Calls ai_query() on enriched context to generate personalized recommendations + email copy | Not Started |
| End-to-end demo notebook | Single walkthrough notebook tying it all together | Not Started |

## Notes

Key demo message: batch `ai_query()` with pre-assembled context is simpler, cheaper, and more auditable than a real-time RAG agent for periodic batch use cases like email campaigns.

---

## Action Items
- [ ] Generate synthetic course data (1,000 accounting/finance courses) — 2026-04-10
- [ ] Generate synthetic customer segmentation data — 2026-04-10
- [ ] Build Vector Search index on course descriptions — 2026-04-10
- [ ] Build batch enrichment pipeline notebook — 2026-04-10
- [ ] Build ai_query recommendation notebook — 2026-04-10
- [ ] Create end-to-end demo notebook — 2026-04-10
- [ ] Test on workspace and share with ACME — 2026-04-10

## Decisions
<!-- Log key decisions here with date and rationale -->

## Meeting Notes
See `meetings/` folder for dated meeting notes.
