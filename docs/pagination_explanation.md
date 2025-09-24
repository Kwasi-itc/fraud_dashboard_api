Pagination model explained
==========================

Client workflow
---------------

1. **First request**

   • Send ordinary query parameters – e.g.  
   `...?page=1&per_page=20` (or omit them to use the defaults).  
   • The API returns the first data page **and** an opaque
   `pagination_token` (base-64 string) in the response metadata.

2. **Subsequent pages**

   • Forget about `page`/`per_page`; just send  
   `...?pagination_token=<token from previous response>`  
   • The back-end decodes the token, uses the embedded DynamoDB
   *LastEvaluatedKey* to continue the scan, and returns the next data
   page together with a fresh `pagination_token`.  
   • Repeat until the token is `null` – meaning no further pages.

Changing *per_page* in the middle of a sequence
-----------------------------------------------

No – once you continue with a `pagination_token` you **must not** alter
`per_page`.  
The token already embeds the paging position derived from the original
page size.  Altering the limit while re-using the token would desynchronise
the cursor and yield inconsistent or duplicated data.

If you really need a different page size you have to
restart the flow: issue a brand-new first request with the new
`per_page` and ignore the old token.
