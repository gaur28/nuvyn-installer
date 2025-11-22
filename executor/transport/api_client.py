"""
API Client for transmitting data to backend
"""

import asyncio
import aiohttp
from typing import Dict, Any, Optional
from executor.config import JobConfig, ConfigManager
from executor.logger import get_logger

logger = get_logger(__name__)


class APIClient:
    """Client for API communication"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.api_endpoint = config_manager.executor_config.api_base_url
        self.api_key = config_manager.get_api_credentials().get('api_key', '')
    
    async def transmit_data(self, job_config: JobConfig) -> Dict[str, Any]:
        """Transmit metadata to backend API"""
        logger.info(f"📤 Transmitting data to API for job: {job_config.job_id}")
        
        try:
            if not self.api_endpoint:
                logger.warning("⚠️ No API endpoint configured - skipping transmission")
                return {
                    "transmission_status": "skipped",
                    "message": "No API endpoint configured"
                }
            
            # Prepare payload
            payload = {
                "job_id": job_config.job_id,
                "tenant_id": job_config.tenant_id,
                "data_source_path": job_config.data_source_path,
                "job_type": job_config.job_type.value,
                "timestamp": asyncio.get_event_loop().time()
            }
            
            # Send to API
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}" if self.api_key else ""
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.api_endpoint}/api/metadata",
                    json=payload,
                    headers=headers,
                    timeout=30
                ) as response:
                    if response.status == 200:
                        result_data = await response.json()
                        logger.info(f"✅ Data transmitted successfully to API")
                        return {
                            "transmission_status": "success",
                            "api_response": result_data,
                            "status_code": response.status
                        }
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ API transmission failed: {response.status} - {error_text}")
                        return {
                            "transmission_status": "failed",
                            "error": f"API returned status {response.status}: {error_text}",
                            "status_code": response.status
                        }
                        
        except asyncio.TimeoutError:
            logger.error("❌ API transmission timeout")
            return {
                "transmission_status": "timeout",
                "error": "API request timed out"
            }
        except Exception as e:
            logger.error(f"❌ API transmission failed: {e}")
            return {
                "transmission_status": "error",
                "error": str(e)
            }
