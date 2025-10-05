"""
Quality Assessor for evaluating data quality
"""

import asyncio
from typing import Dict, Any, List
from config import JobConfig, ConfigManager
from logger import get_logger

logger = get_logger(__name__)


class QualityAssessor:
    """Assesses data quality metrics"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
    
    async def assess_quality(self, job_config: JobConfig) -> Dict[str, Any]:
        """Assess data quality for the given source"""
        logger.info(f"📊 Assessing data quality for: {job_config.data_source_path}")
        
        try:
            # Simulate quality assessment
            quality_metrics = {
                "overall_score": 85,
                "completeness": 90,
                "accuracy": 85,
                "consistency": 80,
                "timeliness": 95,
                "validity": 88,
                "uniqueness": 92
            }
            
            # Calculate overall score
            overall_score = sum(quality_metrics.values()) / len(quality_metrics)
            
            result = {
                "source_path": job_config.data_source_path,
                "assessment_timestamp": asyncio.get_event_loop().time(),
                "quality_metrics": quality_metrics,
                "overall_score": round(overall_score, 2),
                "quality_level": self._get_quality_level(overall_score),
                "recommendations": self._get_recommendations(quality_metrics),
                "assessment_status": "completed"
            }
            
            logger.info(f"✅ Quality assessment completed: {overall_score}/100")
            return result
            
        except Exception as e:
            logger.error(f"❌ Quality assessment failed: {e}")
            return {
                "source_path": job_config.data_source_path,
                "error": str(e),
                "assessment_status": "failed"
            }
    
    def _get_quality_level(self, score: float) -> str:
        """Get quality level based on score"""
        if score >= 90:
            return "Excellent"
        elif score >= 80:
            return "Good"
        elif score >= 70:
            return "Fair"
        elif score >= 60:
            return "Poor"
        else:
            return "Critical"
    
    def _get_recommendations(self, metrics: Dict[str, int]) -> List[str]:
        """Get quality improvement recommendations"""
        recommendations = []
        
        if metrics["completeness"] < 85:
            recommendations.append("Improve data completeness - check for missing values")
        
        if metrics["accuracy"] < 85:
            recommendations.append("Enhance data accuracy - validate data formats and ranges")
        
        if metrics["consistency"] < 85:
            recommendations.append("Improve data consistency - standardize formats and values")
        
        if metrics["uniqueness"] < 90:
            recommendations.append("Address data uniqueness issues - check for duplicates")
        
        if not recommendations:
            recommendations.append("Data quality is good - maintain current standards")
        
        return recommendations
