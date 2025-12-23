from typing import Optional, List, Dict, Any
import asyncio
import aiohttp
import json
from datetime import datetime, timedelta
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration 
STICKY_API_BASE = "" #https://pdfdotnet.sticky.io/api/v1/
STICKY_AUTH = []  # Basic Auth
RATE_LIMIT_PER_MINUTE = 15
REQUEST_TIMEOUT = 36000


import base64

def getUsersPass(cred:str):

    global STICKY_AUTH

    # Decode from Base64
    decoded_bytes = base64.b64decode(cred)
    decoded_str = decoded_bytes.decode('utf-8')

    # Split into user and pass
    if ':' in decoded_str:
        auth = decoded_str.split(':', 1)
        print("Username:", auth[0])
        print("Password:", auth[1])

        return auth
    else:
        print("Invalid format. Expected 'user:pass' after decoding.")

class StickyAPIClient:
    def __init__(self,cred="",company=""):

        global STICKY_API_BASE  # Declare intent to modify the global variable
        global STICKY_AUTH
        STICKY_AUTH = getUsersPass(cred)
        STICKY_API_BASE = f"https://{company}.sticky.io/api/v1/"
        self.session = None
        self.request_count = 0
        self.last_reset = time.time()
        self.semaphore = asyncio.Semaphore(80)  # Limit concurrent requests
        self.request_times = []  # Track request times for sliding window
        self.last_auth = None  # Track last used auth

    async def get_session(self):
        current_auth = aiohttp.BasicAuth(STICKY_AUTH[0], STICKY_AUTH[1])
        
        # Recreate session if auth changed or session doesn't exist
        if self.session is None or self.last_auth != current_auth:
            if self.session is not None:
                await self.session.close()
            
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                auth=current_auth,
                headers={"Content-Type": "application/json"}
            )
            self.last_auth = current_auth
        
        return self.session
    
    async def rate_limit_check_sliding_window(self):
        """Sliding window rate limiting - more efficient"""
        current_time = time.time()
        
        # Remove requests older than 1 minute
        self.request_times = [req_time for req_time in self.request_times 
                             if current_time - req_time < 60]
        
        # If we're at the limit, wait until the oldest request expires
        if len(self.request_times) >= 15:
            wait_time = 60 - (current_time - self.request_times[0]) + 0.1
            if wait_time > 0:
                await asyncio.sleep(wait_time)
                # Clean up expired requests after waiting
                current_time = time.time()
                self.request_times = [req_time for req_time in self.request_times 
                                     if current_time - req_time < 60]
        
        # Record this request
        self.request_times.append(current_time)
    
    async def call_order_view_batch(self, batch: List[str], batch_num: int, bIsOriginal: bool = True) -> List[Dict]:
        """Process a single batch with proper rate limiting"""
        async with self.semaphore:  # Limit concurrent batches
            max_retries = 3
            retry_delay = 1
            
            logger.info(f"Processing batch {batch_num}: {len(batch)} orders")
            
            for attempt in range(max_retries):
                try:
                    await self.rate_limit_check_sliding_window()
                    
                    payload = {
                        "order_id": [int(oid) for oid in batch],
                        "return_variants": 1
                    }
                    
                    session = await self.get_session()
                    async with session.post(
                        f"{STICKY_API_BASE}order_view",
                        json=payload
                    ) as response:
                        if response.status != 200:
                            error_text = await response.text()
                            if attempt == max_retries - 1:
                                logger.error(f"Batch {batch_num} failed after {max_retries} attempts: {error_text}")
                                return []
                            else:
                                logger.warning(f"Batch {batch_num} failed, attempt {attempt + 1}: {error_text}")
                                await asyncio.sleep(retry_delay)
                                retry_delay *= 2
                                continue
                        
                        batch_result = await response.json()
                        
                        # Transform orders (your existing logic)
                        orders = list(batch_result.get("data", {}).values()) if isinstance(batch_result.get("data"), dict) else [batch_result]
                        transformed_orders = []

                        for order in orders:
                            order_copy = order.copy()
                            products = order_copy.pop("products", [])

                            if isinstance(products, list) and len(products) == 1:
                                product = products[0]
                                for key, value in product.items():
                                    if isinstance(value, dict):
                                        for key_1, value_1 in value.items():
                                            order_copy[f"prd_{key}_{key_1}"] = value_1
                                    else:
                                        order_copy[f"prd_{key}"] = value

                            if "totals_breakdown" in order_copy and isinstance(order_copy["totals_breakdown"], dict):
                                breakdown = order_copy.pop("totals_breakdown")
                                for k, v in breakdown.items():
                                    order_copy[f"breakdown_{k}"] = v

                            order_copy.pop("utm_info", None)
                            if bIsOriginal:
                                order_copy.pop("order_customer_types", None)

                            if "systemNotes" in order_copy and isinstance(order_copy["systemNotes"], list):
                                order_copy["systemNotes"] = ",".join(map(str, order_copy["systemNotes"]))
                            
                            order_copy["source"] = "original" if bIsOriginal else "updated"
                            
                            transformed_orders.append(order_copy)

                        # Cache the results
                        for order in transformed_orders:
                            order_id = str(order.get("order_id"))
                        
                        logger.info(f"Batch {batch_num} completed successfully: {len(transformed_orders)} orders")
                        return transformed_orders
                        
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Batch {batch_num} failed after {max_retries} attempts: {str(e)}")
                        return []
                    else:
                        logger.warning(f"Batch {batch_num} error, attempt {attempt + 1}: {str(e)}")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
            
            return []
    
    async def call_order_view_concurrent(self, order_ids: List[str], isOriginal:bool = True) -> List[Dict[str, Any]]:
        """Call order_view API with concurrent batch processing"""
        uncached_ids = []
        
        for order_id in order_ids:
                uncached_ids.append(order_id)
        

        
        # Create batches of 500
        batch_size = 500
        batches = []
        for i in range(0, len(uncached_ids), batch_size):
            batch = uncached_ids[i:i + batch_size]
            batches.append((batch, i//batch_size + 1))
        
        logger.info(f"Processing {len(batches)} batches concurrently with rate limit of 80/min")
        
        # Process batches concurrently
        tasks = []
        for batch, batch_num in batches:
            task = asyncio.create_task(
                self.call_order_view_batch(batch, batch_num,isOriginal)
            )
            tasks.append(task)
        
        # Wait for all batches to complete
        all_orders = []
        completed_batches = 0
        
        for coro in asyncio.as_completed(tasks):
            try:
                batch_result = await coro
                all_orders.extend(batch_result)
                completed_batches += 1
                logger.info(f"Progress: {completed_batches}/{len(batches)} batches completed")
            except Exception as e:
                logger.error(f"Task failed: {str(e)}")
                completed_batches += 1
        
        # Combine cached and fresh orders
        logger.info(f"Total orders retrieved: {len(all_orders)} ")      
        return all_orders
    
    async def call_order_find(
        self, 
        start_date: str, 
        end_date: str,
        start_time: str = "00:00:00",
        end_time: str = "23:59:59", 
        campaign_id: str = "all",
        date_type: str = "create",
        criteria: str = "all",
        search_type: str = "all"
    ) -> List[Dict]:
        """Call Sticky.io order_find API with retry logic"""

        max_retries = 3
        retry_delay = 5

        payload = {
            "campaign_id": campaign_id,
            "start_date": start_date,
            "end_date": end_date,
            "start_time": start_time,
            "end_time": end_time,
            "date_type": date_type,
            "criteria": criteria,
            "search_type": search_type
        }

        logger.info(f"Calling order_find with payload: {payload}")

        for attempt in range(max_retries):
            try:
                await self.rate_limit_check_sliding_window()

                session = await self.get_session()
                async with session.post(
                    f"{STICKY_API_BASE}order_find",
                    json=payload
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        if attempt == max_retries - 1:
                            logger.error(f"order_find failed after {max_retries} attempts: {error_text}")
                            raise HTTPException(
                                status_code=response.status,
                                detail=f"Sticky.io API error: {error_text}"
                            )
                        else:
                            logger.warning(f"order_find failed, attempt {attempt + 1}: {error_text}")
                            await asyncio.sleep(retry_delay)
                            retry_delay *= 2
                            continue
                        
                    return await response.json()

            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"order_find failed after {max_retries} attempts: {str(e)}")
                    raise HTTPException(status_code=500, detail=f"API error: {str(e)}")
                else:
                    logger.warning(f"order_find error, attempt {attempt + 1}: {str(e)}")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
    

    async def call_order_find_updated(
        self, 
        start_date: str, 
        end_date: str,
        start_time: str = "00:00:00",
        end_time: str = "23:59:59", 
        campaign_id: str = "all",
        group_keys: List[str] = ["chargeback","confirmation","fraud","refund","reprocess","return","rma","void"]
    ) -> List[Dict]:
        """Call Sticky.io order_find API"""
        await self.rate_limit_check_sliding_window()
        
        payload = {
            "campaign_id": campaign_id,
            "start_date": start_date,
            "end_date": end_date,
            "start_time": start_time,
            "end_time": end_time,
            "group_keys": group_keys
        }
        
        logger.info(f"Calling order_find with payload: {payload}")
        
        session = await self.get_session()
        async with session.post(
            f"{STICKY_API_BASE}order_find_updated",
            json=payload
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"order_find API error: {error_text}")
                raise HTTPException(
                    status_code=response.status,
                    detail=f"Sticky.io API error: {error_text}"
                )
            return await response.json()
        
    async def call_order_view(self, order_ids: List[str]) -> Dict[str, Any]:
        """Call Sticky.io order_view API for multiple orders with 500 order limit and retry logic"""
        
        uncached_ids = []
        
        for order_id in order_ids:
            uncached_ids.append(order_id)
        
        
        # Fetch uncached orders in batches of 500 (Sticky.io limit)
        batch_size = 500
        all_orders = []
        
        for i in range(0, len(uncached_ids), batch_size):
            batch = uncached_ids[i:i + batch_size]
            max_retries = 3
            retry_delay = 1
            
            logger.info(f"Processing batch {i//batch_size + 1}: {len(batch)} orders")
            
            for attempt in range(max_retries):
                try:
                    await self.rate_limit_check_sliding_window()
                    
                    payload = {
                        "order_id": [int(oid) for oid in batch],
                        "return_variants": 1
                    }
                    
                    session = await self.get_session()
                    async with session.post(
                        f"{STICKY_API_BASE}order_view",
                        json=payload
                    ) as response:
                        if response.status != 200:
                            error_text = await response.text()
                            if attempt == max_retries - 1:
                                logger.error(f"Order view failed for batch after {max_retries} attempts: {error_text}")
                                continue
                            else:
                                logger.warning(f"Order view failed, attempt {attempt + 1}: {error_text}")
                                await asyncio.sleep(retry_delay)
                                retry_delay *= 2
                                continue
                        
                        batch_result = await response.json()
                        
                        # Process orders
                        orders = list(batch_result.get("data", {}).values()) if isinstance(batch_result.get("data"), dict) else [batch_result]
                        transformed_orders = []

                        for order in orders:
                            order_copy = order.copy()
                            products = order_copy.pop("products", [])

                            if isinstance(products, list) and len(products) == 1:
                                product = products[0]
                                for key, value in product.items():
                                    if isinstance(value, dict):
                                        for key_1, value_1 in value.items():
                                            order_copy[f"prd_{key}_{key_1}"] = value_1
                                    else:
                                        order_copy[f"prd_{key}"] = value

                            if "totals_breakdown" in order_copy and isinstance(order_copy["totals_breakdown"], dict):
                                breakdown = order_copy.pop("totals_breakdown")
                                for k, v in breakdown.items():
                                    order_copy[f"breakdown_{k}"] = v

                            order_copy.pop("utm_info", None)
                            order_copy.pop("custom_fields", None)
                            order_copy.pop("order_customer_types", None)

                            if "systemNotes" in order_copy and isinstance(order_copy["systemNotes"], list):
                                order_copy["systemNotes"] = ",".join(map(str, order_copy["systemNotes"]))

                            transformed_orders.append(order_copy)


                        # Handle single vs multiple order responses
                        if isinstance(transformed_orders, list):
                            for order in transformed_orders:
                                order_id = str(order.get("order_id"))
                                all_orders.append(order)
                        else:
                            logger.error(f"Error in transformation")
                            raise Exception("Error in transformation")
                        
                        break  # Success, break out of retry loop
                        
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Order view failed after {max_retries} attempts: {str(e)}")
                    else:
                        logger.warning(f"Order view error, attempt {attempt + 1}: {str(e)}")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
        
        return all_orders

    # 🆕 NEW METHODS FOR CHUNKING SOLUTION
    def convert_order_timestamp_to_find_params(self, timestamp: str) -> tuple[str, str]:
        """
        Convert order_view timestamp to order_find format
        Input: "2024-09-12 00:39:54" (from order_view)
        Output: ("09/12/2024", "00:39:54") (for order_find)
        """
        try:
            dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            start_date = dt.strftime("%m/%d/%Y")
            start_time = dt.strftime("%H:%M:%S")
            return start_date, start_time
        except ValueError as e:
            logger.error(f"Error converting timestamp {timestamp}: {e}")
            raise ValueError(f"Invalid timestamp format: {timestamp}")

    async def call_order_find_complete(
        self, 
        start_date: str, 
        end_date: str,
        start_time: str = "00:00:00",
        end_time: str = "23:59:59", 
        campaign_id: str = "all",
        date_type: str = "create",
        criteria: str = "all",
        search_type: str = "all"
    ) -> tuple[List[str], int]:
        """
        🆕 Complete order_find that handles 50K+ order limits by intelligent chunking
        Uses actual order timestamps to determine chunk boundaries.
        Returns: (all_order_ids, total_orders_found)
        """
        all_order_ids = []
        current_start_date = start_date
        current_start_time = start_time
        chunk_count = 0
        total_orders_reported = 0
        
        logger.info(f"🔄 Starting complete order_find from {start_date} {start_time} to {end_date} {end_time}")
        
       
        while True:
            chunk_count += 1
            
            # Call order_find for current chunk
            logger.info(f"📋  Chunk {chunk_count}: Calling order_find from {current_start_date} {current_start_time}")
            
            response = await self.call_order_find(
                start_date=current_start_date,
                end_date=end_date,
                start_time=current_start_time,
                end_time=end_time,
                campaign_id=campaign_id,
                date_type=date_type,
                criteria=criteria,
                search_type=search_type
            )
            
            order_ids = response.get("order_id", [])
            total_orders_chunk = int(response.get("total_orders", "0"))
            
            # Store the total from first chunk (this is the real total we're aiming for)
            if chunk_count == 1:
                total_orders_reported = total_orders_chunk
                logger.info(f"🎯 Target total orders: {total_orders_reported}")
            
            logger.info(f"📦 Chunk {chunk_count}: Got {len(order_ids)} order IDs")
            
            # Add orders from this chunk
            all_order_ids.extend(order_ids)
            
            # Check if we've reached the end
            if len(order_ids) < 50000 and len(order_ids) == total_orders_chunk :
                logger.info(f"✅ Final chunk {chunk_count}: {len(order_ids)} orders (< 50K limit)")
                break
            
            # Check if we hit exactly 50K - likely truncated
            if len(order_ids) == 50000:
                logger.info(f"⚠️  Chunk {chunk_count}: Got exactly 50K orders - likely truncated, continuing...")
                
                # Get timestamp of last order in this batch
                last_order_id = str(order_ids[-1])
                logger.info(f"🔍 Getting timestamp for last order: {last_order_id}")
                
                try:
                    # Call order_view to get timestamp of last order
                    last_order_details = await self.call_order_view([last_order_id])
                    
                    if not last_order_details or len(last_order_details) == 0:
                        logger.error(f"❌ Could not get details for last order {last_order_id}")
                        break
                    
                    last_timestamp = last_order_details[0].get("time_stamp")
                    
                    if not last_timestamp:
                        logger.error(f"❌ No timestamp found for order {last_order_id}")
                        break
                    
                    logger.info(f"📅 Last order timestamp: {last_timestamp}")
                    
                    # Convert to order_find format  
                    current_start_date, current_start_time = self.convert_order_timestamp_to_find_params(last_timestamp)
                    
                    logger.info(f"➡️  Next chunk will start from: {current_start_date} {current_start_time}")
                    
                except Exception as e:
                    logger.error(f"❌ Error getting timestamp for order {last_order_id}: {str(e)}")
                    break
            else:
                # Got less than 50K orders, we're done
                break
        
        # Deduplicate while preserving order (Option A approach)
        logger.info(f"🔄 Deduplicating {len(all_order_ids)} orders...")
        unique_order_ids = list(dict.fromkeys(all_order_ids))
        duplicates_removed = len(all_order_ids) - len(unique_order_ids)
        
        logger.info(f"   ✅ Complete order_find finished:")
        logger.info(f"   📊 Total chunks: {chunk_count}")
        logger.info(f"   📋 Orders collected: {len(all_order_ids)}")
        logger.info(f"   🎯 Unique orders: {len(unique_order_ids)}")
        logger.info(f"   🔄 Duplicates removed: {duplicates_removed}")
        logger.info(f"   📈 Target vs Actual: {total_orders_reported} vs {len(unique_order_ids)}")
        
        # Verification
        if len(unique_order_ids) != total_orders_reported:
            logger.warning(f"⚠️  Mismatch: Expected {total_orders_reported}, got {len(unique_order_ids)} unique orders")
        else:
            logger.info(f"✅ Perfect match: Got all {total_orders_reported} orders!")
        
        result = (unique_order_ids,total_orders_reported)
        
        return unique_order_ids, total_orders_reported

    async def call_order_find_updated_complete(
        self, 
        start_date: str, 
        end_date: str,
        start_time: str = "00:00:00",
        end_time: str = "23:59:59", 
        campaign_id: str = "all",
        group_keys: List[str] = ["chargeback","confirmation","fraud","refund","reprocess","return","rma","void"]
    ) -> tuple[List[str], int]:
        """
        🆕 Complete order_find that handles 50K+ order limits by intelligent chunking
        Uses actual order timestamps to determine chunk boundaries.
        Returns: (all_order_ids, total_orders_found)
        """
        all_order_ids = []
        current_start_date = start_date
        current_start_time = start_time
        chunk_count = 0
        total_orders_reported = 0
        
        logger.info(f"🔄 Starting complete order_find from {start_date} {start_time} to {end_date} {end_time}")
        
        while True:
            chunk_count += 1
            
            # Call order_find for current chunk
            logger.info(f"📋 Chunk {chunk_count}: Calling order_find from {current_start_date} {current_start_time}")
            
            response = await self.call_order_find_updated(
                start_date=current_start_date,
                end_date=end_date,
                start_time=current_start_time,
                end_time=end_time,
                campaign_id=campaign_id,
                group_keys = group_keys
            )
            
            order_ids = response.get("order_id", [])
            total_orders_chunk = int(response.get("total_orders", "0"))
            
            # Store the total from first chunk (this is the real total we're aiming for)
            if chunk_count == 1:
                total_orders_reported = total_orders_chunk
                logger.info(f"🎯 Target total orders: {total_orders_reported}")
            
            logger.info(f"📦 Chunk {chunk_count}: Got {len(order_ids)} order IDs")
            
            # Add orders from this chunk
            all_order_ids.extend(order_ids)
            
            # Check if we've reached the end
            if len(order_ids) < 50000 and len(order_ids) == total_orders_chunk :
                logger.info(f"✅ Final chunk {chunk_count}: {len(order_ids)} orders (< 50K limit)")
                break
            
            # Check if we hit exactly 50K - likely truncated
            if len(order_ids) == 50000:
                logger.info(f"⚠️  Chunk {chunk_count}: Got exactly 50K orders - likely truncated, continuing...")
                
                # Get timestamp of last order in this batch
                last_order_id = str(order_ids[-1])
                logger.info(f"🔍 Getting timestamp for last order: {last_order_id}")
                
                try:
                    # Call order_view to get timestamp of last order
                    last_order_details = await self.call_order_view([last_order_id])
                    
                    if not last_order_details or len(last_order_details) == 0:
                        logger.error(f"❌ Could not get details for last order {last_order_id}")
                        break
                    
                    last_timestamp = last_order_details[0].get("time_stamp")
                    
                    if not last_timestamp:
                        logger.error(f"❌ No timestamp found for order {last_order_id}")
                        break
                    
                    logger.info(f"📅 Last order timestamp: {last_timestamp}")
                    
                    # Convert to order_find format  
                    current_start_date, current_start_time = self.convert_order_timestamp_to_find_params(last_timestamp)
                    
                    logger.info(f"➡️  Next chunk will start from: {current_start_date} {current_start_time}")
                    
                except Exception as e:
                    logger.error(f"❌ Error getting timestamp for order {last_order_id}: {str(e)}")
                    break
            else:
                # Got less than 50K orders, we're done
                break
        
        # Deduplicate while preserving order (Option A approach)
        logger.info(f"🔄 Deduplicating {len(all_order_ids)} orders...")
        unique_order_ids = list(dict.fromkeys(all_order_ids))
        duplicates_removed = len(all_order_ids) - len(unique_order_ids)
        
        logger.info(f"   ✅ Complete order_find_updated finished:")
        logger.info(f"   📊 Total chunks: {chunk_count}")
        logger.info(f"   📋 Orders collected: {len(all_order_ids)}")
        logger.info(f"   🎯 Unique orders: {len(unique_order_ids)}")
        logger.info(f"   🔄 Duplicates removed: {duplicates_removed}")
        logger.info(f"   📈 Target vs Actual: {total_orders_reported} vs {len(unique_order_ids)}")
        
        # Verification
        if len(unique_order_ids) != total_orders_reported:
            logger.warning(f"⚠️  Mismatch: Expected {total_orders_reported}, got {len(unique_order_ids)} unique orders")
        else:
            logger.info(f"✅ Perfect match: Got all {total_orders_reported} orders!")
        
        return unique_order_ids, total_orders_reported
    
