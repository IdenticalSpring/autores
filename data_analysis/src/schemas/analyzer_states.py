from pydantic import BaseModel, Field, field_validator, ConfigDict, PrivateAttr, model_validator
from typing import Optional, Annotated, Union, ForwardRef, Any
from typing import Annotated
import pandas as pd
from enum import Enum
from langgraph.graph import MessagesState
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_openai import ChatOpenAI
from langgraph.graph.message import add_messages
from langchain_core.messages import AnyMessage
import io
import json
import logging
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
import operator
from typing_extensions import TypedDict

# Set up logging
logger = logging.getLogger(__name__)



def append_list(left: list, right: list) -> list:
    """Append right list to left list."""
    return left + right

######### Search Doc Classes ###########

class Subsection(BaseModel):
    title: str
    content: str
    keywords: list[str] = Field(default_factory=list)

class Doc(BaseModel):
    id: str
    title: str
    keywords: list[str] = Field(default_factory=list)
    content: str
    variable_content: Optional[str] = None
    subsections: list[Subsection] = Field(default_factory=list)

class Query(BaseModel):
    keywords: Optional[list[str]] = Field(default_factory=list)

######### Analyze Tool Classes ###########

class TransformationRecord(BaseModel):
    transform_type: str  # e.g., "log", "differencing", "standard_scale", "box_cox"
    # Parameters needed for the transformation AND its inverse
    # For 'log', 'sqrt': no specific params needed for inverse beyond knowing the type.
    # For 'box_cox': {'lambda_val': float}
    # For 'differencing': {'d': int, 'D': int, 'm': int, 'head_values_for_inversion': dict[str, Any]}
    # For 'scaling': {'scaler_object': object} # The fitted scaler
    # For 'dummy_creation_from_categorical': {'original_variable_name': str, 'prefix': str, 'drop_first': bool, 'categories_created': list[str]}
    # For 'decomposition': {'original_variable_name': str, 'model_type': str, 'period': int, 'component_names': dict[str,str]}
    # For 'aggregation': {'original_variable_name': str, 'original_frequency': str, 'target_frequency': str, 'agg_method': str}
    params: dict[str, Any]
    applied_to_variable: str # Name of the variable this transform was applied to, or original name if new vars created
    created_variables: Optional[list[str]] = None # Names of new variables if the transform created them

class VariableType(str, Enum):
    OBSERVED = "observed" # (the most common variable type)
    LATENT = "latent" # not has the actual record on data
    TIME_INDEX = "time_index"
    ENTITY_INDEX = "entity_index"
    COMPONENT = "component" # e.g., trend, seasonal, residual from decomposition

class VariableRole(str, Enum):
    DEPENDENT = "dependent"
    INDEPENDENT = "independent" # by default consider this as independent exogenous
    INTERMEDIATE = "intermediate"
    MODERATOR = "moderator"
    CONTROL = "control"
    ENDOGENOUS = "endogenous" # or can be understood as independent endogenous
    NULL = "null"

class ScaleType(str, Enum):
    """Enum for scale types supported in the autoresearching system."""
    NOMINAL = "nominal"
    ORDINAL = "ordinal"
    INTERVAL = "interval"
    RATIO = "ratio"
    GUTTMAN = "guttman"
    SEMANTIC_DIFFERENTIAL = "semantic_differential"
    HYBRID = "hybrid"
    PERFORMANCE_LEVEL = "performance_level"
    
class Variable(BaseModel):
    name: str
    code: str
    variable_type: VariableType = VariableType.OBSERVED
    role: VariableRole = VariableRole.INDEPENDENT
    parent_code: Optional[str] = None  # For grouping variables
    statement: Optional[str] = None  # Description or question text
    values: Optional[str] = None  # Possible values for categorical variables, or range
    scale: ScaleType  # e.g., "interval", "nominal", "ordinal", "ratio", "guttman", "semantic differential", "hybrid", "performance level"
    unit: Optional[str] = None # e.g., "USD", "count", date format it can be "DayOfWeek", "DayOfMonth", "Month", "Hour", "%YYYY-%mm-%dd" ...
    collection_method: Optional[str] = None  # e.g., "survey", "computed"

    properties: dict[str, Any] = Field(default_factory=dict) # e.g., {'seasonal_period': 12, 'is_stationary': False} {measurement_type: reflective / formative}
    transform_history: list[TransformationRecord] = Field(default_factory=list)
    source_variable_code: Optional[str] = None # If derived, points to original variables

class ActionType(str, Enum):
    """
    Enum defining types of actions that affect data during analysis.
    
    Attributes:
        REMOVE_VARIABLE: Action to remove a single variable from the dataset.
        DISCARD_FACTOR: Action to discard an entire factor from analysis.
        RECHECK_DATA: User needs to recheck the data for a specific issue.
        REANALYZE: Action to trigger a reflection tool without modifying the dataset.
        SPLIT_VARIABLE: Dividing a single variable into multiple new variables.
        COMBINE_VARIABLES: Merging several variables into one composite variable.
        TRANSFORM_VARIABLES: Applying mathematical functions (e.g., logarithm, square root, normalization, differencing).
        INVERSE_TRANSFORM_VARIABLES : Reverting data transformations (e.g., log to original scale) for interpretation.
        HANDLE_OUTLIERS: Detecting and removing or adjusting unusual values.
        HANDLE_MISSING_VALUES: Imputing, replacing, or removing absent data points.
        HANDLE_DUPLICATES = "handle_duplicates"
        CONVERT_DATA_TYPE = "convert_data_type"
        
    """
    REMOVE_VARIABLE = "remove_variable"
    DISCARD_FACTOR = "discard_factor"
    RECHECK_DATA = "recheck_data"
    REANALYZE = "reanalyze"
    
    SPLIT_VARIABLE = "split_variable"
    COMBINE_VARIABLES = "combine_variables"

    TRANSFORM_VARIABLES = "transform_variables"
    INVERSE_TRANSFORM_VARIABLES = "inverse_transform_variables"

    HANDLE_OUTLIERS = "handle_outliers"
    HANDLE_MISSING_VALUES = "handle_missing_values"

    MODIFY_MODEL_SPECIFICATION = "modify_model_specification"
    
    UPDATE_VARIABLE_PROPERTIES = "update_variable_properties"
    
    HANDLE_DUPLICATES = "handle_duplicates"
    CONVERT_DATA_TYPE = "convert_data_type"
    

class Action(BaseModel):
    """
    Represents an action to modify data, such as removing a variable or discarding a factor, suggested by an analysis method to resolve an issue.
    Actions affect the dataset and may trigger reflection steps to re-evaluate the analysis.

    Attributes:
        action_type: Type of action to perform (e.g., REMOVE_VARIABLE, DISCARD_FACTOR).
        method: Analysis method that suggested the action (e.g., 'reliability_analysis', 'run_data_preprocessing').
        issue: Description of the problem prompting the action (e.g., 'CITC = 0.075 < 0.3', as much detailed as possible).
        comment: Suggested next steps or rationale for the action (e.g., 'Recheck data after removal').
        status: Approval state of the action (e.g., 'pending' -> 'approved' -> 'done' or 
                                                'pending' -> 'rejected' -> 'skipped' ).
        action_params: Parameters required to execute the action.
        reflection_params: Optional parameters to trigger a reflection step after the action.
        reset_actions: Whether to reset the action queue after this action.
        
    Action Parameters Format by Action Type:
    
    Basic Variable Operations:
    
    REMOVE_VARIABLE: {'variable': 'DN1'}
    
    DISCARD_FACTOR: {'factor': 'BC'}
    
    RECHECK_DATA: {'variables': ['DN1', 'DN2'], 'issue_type': 'missing_values'}
    
    Variable Structure Operations:
    
    SPLIT_VARIABLE (Seasonal Decomposition):
    {'variable_to_split': 'gdp', 'method': 'seasonal_decompose', 'model_type': 'additive', 
     'period': 4, 'output_component_names': {'trend': 'gdp_trend', 'seasonal': 'gdp_seasonal', 'resid': 'gdp_resid'}}
    
    SPLIT_VARIABLE (Dummification):
    {'variable_to_split': 'region_categorical', 'method': 'dummify', 'drop_first': True, 'prefix': 'region'}
    
    COMBINE_VARIABLES (Sum):
    {'input_variables': ['sales_east', 'sales_west'], 'output_variable_name': 'total_sales', 'method': 'sum'}
    
    COMBINE_VARIABLES (Weighted):
    {'input_variables': ['sales_east', 'sales_west'], 'output_variable_name': 'weighted_sales', 
     'method': 'sum', 'weights': [0.6, 0.4]}
    
    Variable Transformation Operations:
    
    TRANSFORM_VARIABLES (Log):
    {'variable': 'price', 'transform_type': 'log', 'transform_params': {'base': 'natural', 'offset': 0}}
    
    TRANSFORM_VARIABLES (Square Root):
    {'variable': 'count_data', 'transform_type': 'square_root'}
    
    TRANSFORM_VARIABLES (Box-Cox):
    {'variable': 'skewed_data', 'transform_type': 'box_cox', 'transform_params': {'lambda': 0.5}}
    
    TRANSFORM_VARIABLES (Differencing):
    {'variable': 'stock_price', 'transform_type': 'differencing', 'transform_params': {'d': 1, 'D': 1, 'm': 12}}
    
    TRANSFORM_VARIABLES (Scaling):
    {'variable': 'feature_A', 'transform_type': 'standard_scale'}
    
    INVERSE_TRANSFORM_VARIABLES (Inverse Log):
    {'variable': 'log_forecast_price', 'original_transform_type': 'log', 'transform_params': {'base': 'natural', 'offset': 0}}
    
    INVERSE_TRANSFORM_VARIABLES (Inverse Differencing):
    {'variable': 'diff_forecast_stock', 'original_transform_type': 'differencing', 
     'transform_params': {'d': 1, 'D': 1, 'm': 12, 'initial_values': [100.5, 101.2, 99.8]}}
    
    INVERSE_TRANSFORM_VARIABLES (Inverse Scaling):
    {'variable': 'scaled_forecast', 'original_transform_type': 'standard_scale', 'transform_params': {'mean': 50.0, 'std': 10.0}}
    
    CONVERT_DATA_TYPE: {'variable': 'region_code', 'target_type': 'category', 'current_type': 'int64'}
    
    Data Quality Operations:
    
    HANDLE_OUTLIERS (Remove):
    {'variable': 'revenue', 'method': 'remove', 'outlier_count': 15, 'outlier_indices': [45, 67, 89], 
     'detection_method': 'iqr', 'detection_params': {'multiplier': 1.5, 'threshold': 3.0}}
    
    HANDLE_OUTLIERS (Cap/Floor):
    {'variable': 'revenue', 'method': 'cap_floor', 'outlier_count': 15, 'outlier_indices': [45, 67, 89],
     'method_params': {'lower_percentile': 5, 'upper_percentile': 95}}
    
    HANDLE_OUTLIERS (Transform):
    {'variable': 'revenue', 'method': 'transform', 'transform_type': 'log'}
    
    HANDLE_MISSING_VALUES (Remove):
    {'variable': 'temperature', 'method': 'remove', 'missing_count': 25, 'removal_strategy': 'listwise'}
    
    HANDLE_MISSING_VALUES (Statistical):
    {'variable': 'temperature', 'method': 'mean', 'missing_count': 25}
    
    HANDLE_MISSING_VALUES (Interpolation):
    {'variable': 'temperature', 'method': 'linear_interpolate', 'missing_count': 25, 
     'method_params': {'order': 2, 'limit': 5}}
    
    HANDLE_MISSING_VALUES (Forward Fill):
    {'variable': 'product_category', 'method': 'forward_fill', 'missing_count': 25, 'method_params': {'limit': 3}}
    
    HANDLE_MISSING_VALUES (KNN Imputation):
    {'variable': 'feature_X', 'method': 'knn_impute', 'missing_count': 25, 'method_params': {'n_neighbors': 5, 'weights': 'uniform'}}
    
    HANDLE_DUPLICATES: {'operation': 'remove_duplicates', 'duplicate_count': 15}
    
    Model and Analysis Operations:
    
    MODIFY_MODEL_SPECIFICATION:
    {'model_type': 'arima', 'current_specification': {'p': 1, 'd': 1, 'q': 1}, 
     'suggested_specification': {'p': 2, 'd': 1, 'q': 1}, 
     'modification_reason': 'Significant partial autocorrelation at lag 2'}
                               
    UPDATE_VARIABLE_PROPERTIES:
    {'variable_codes': ['GDP', 'CPI'], 'properties_to_update': {'seasonal_period': 12, 
     'is_stationary': False, 'transformation_applied': 'log', 'outlier_treatment': 'capped'}}
    
    REANALYZE:
    {'trigger_reason': 'Data transformations applied', 'target_variables': ['GDP', 'CPI'], 'analysis_scope': 'full'}
    
    Reflection Parameters:
    reflection_params = {'tool': 'analyze_single_factor', 'parameters': {'factor_key': 'DN', 'target_variables': ['DN1', 'DN2']}, 
                        'comment': 'Re-run reliability analysis after removing DN1', 'reset_actions': False, 'recursive': False}
    """
    action_type: ActionType
    method: str
    issue: str
    comment: str
    status: str
    action_params: Optional[dict] = None
    reflection_params: Optional[dict] = None
    reset_actions: bool = False
    
    @field_validator("status")
    def validate_status(cls, v):
        valid_statuses = {"pending", "approved", "rejected", "done", "skipped", "failed"}
        if v not in valid_statuses:
            raise ValueError(f"Status must be one of {valid_statuses}")
        return v

class ToolOutput(BaseModel):
    """
    Represents the output of a tool execution, including results, logs, files, and suggested actions.

    Attributes:
        results: dictionary containing the main analysis results.
        logs: list of log messages for reporting.
        file_contents: dictionary mapping file paths (relative) to their contents (e.g., CSV text).
        file_descriptions: dictionary mapping file paths to their descriptions (returned from get_file_description).
        action: Optional list of suggested actions that affect the dataset.
    """
    results: dict
    logs: list[str] # Source of content is here
    file_contents: Optional[dict[str, str]] = None
    file_descriptions: Optional[dict[str, dict[str, str]]] = None
    action: Optional[list[Action]] = None
    # @field_validator("results")
    # def validate_results(cls, v):
    #     try:
    #         json.dumps(v)  # Test for JSON serializability
    #         return v
    #     except TypeError:
    #         raise ValueError("Results must be JSON-serializable")

class AnalysisStepType(str, Enum):
    """
    Enum defining the type of step in a pipeline.

    Attributes:
        TOOL: A tool function (e.g., analyze_single_factor).
        ACTION: An action (e.g., REMOVE_VARIABLE).
        REFLECTION: A reflection step triggered by an action (e.g., re-run a tool).
    """
    TOOL = "tool"
    ACTION = "action"
    REFLECTION = "reflection"

class ExecutionMode(str, Enum):
    """
    Enum defining the execution mode of the pipeline.
    AUTO: Automatically approve actions and continue.
    INTERACTIVE: Pause for user approval on pending actions.
    """
    AUTO = "auto" # auto accept everything
    INTERACTIVE = "interactive" # need to interact everything possible
    MANUAL = "manual"

class AnalyzeStep(BaseModel):
    """
    Represents a single step in a pipeline, which can be a tool, action, or reflection.

    Attributes:
        step_type: Indicates the type of step (TOOL, ACTION, or REFLECTION).
        tool: Name of the tool function (e.g., "analyze_single_factor") if step_type is TOOL or REFLECTION.
        action_type: Type of action (e.g., REMOVE_VARIABLE) if step_type is ACTION.
        parameters: Parameters specific to the tool or action.
                   - For TOOL/REFLECTION: Tool-specific params (e.g., {"factor_key": "DN"}).
                   - For ACTION: Action parameters (e.g., {"action": action.model_dump()} ).
        comment: Description of the step's purpose (e.g., "Perform reliability analysis").
        reset_actions: reset to clear steps (same tool name only) right after in pipeline (can be raised from the actions returned by tools)
        recursive: for type TOOL/ REFLECTION, to allow this reflection to append the actions in the main pipeline or not 
        output: Result of executing the step (e.g., ToolOutput for tools, dict for actions).
        plan_id: Identifies the plan this step belongs to (e.g., "plan_001").
    """
    
    step_type: Optional[AnalysisStepType] = None
    tool: Optional[str] = None
    action_type: Optional[ActionType] = None
    parameters: Optional[dict] = None
    comment: Optional[str] = None
    reset_actions: bool = False
    recursive: bool = False
    output: Optional[Union[ToolOutput, dict]] = None
    plan_id: int

class Plan(BaseModel):
    """
    Represents an AI-generated pipeline of steps to execute an analysis.

    Attributes:
        analysis_description: The type of analysis (e.g., "factor_regression", "efa").
        steps: list of steps (initially TOOL steps) defining the pipeline.
        parameters: Global parameters applicable to all steps (e.g., {"output_dir": "analysis"}).
    """
    plan_id: int
    analysis_description: str
    steps: list[AnalyzeStep]
    parameters: Optional[dict] = None
    accepted: bool = False

class SimplifiedPlanStep(BaseModel):
    # `parameters` do LLM sinh tự do ⇒ dùng dict mở. Union chặt + additionalProperties
    # False trước đây khiến 1 giá trị lạ (bool/list[int]/list[dict]) làm hỏng cả plan.
    model_config = ConfigDict(extra="allow")
    tool: str = Field(description="Name of the tool to execute (e.g., 'run_reliability_analysis').")
    parameters: Optional[dict] = Field(
        default=None,
        description=(
            "Parameters specific to the tool, if any. Values can be integers, floats, strings, "
            "lists or nested dictionaries — không ràng buộc kiểu ở tầng schema."
        ),
    )

class SimplifiedPlan(BaseModel):
    analysis_description: str = Field(description="Description of the analysis method (e.g., 'factor_regression', 'efa').")
    steps: list[SimplifiedPlanStep] = Field(description="list of steps defining the tools and their parameters.")

#############################################

class ExecutionLog(BaseModel):
    """
    Tracks the execution of a pipeline, including tools, actions, and reflections.
    Manages pending steps in a queue/stack.
    """
    plans: list[Plan] = Field(default_factory=list)
    executed_steps: list[AnalyzeStep] = Field(default_factory=list)
    pending_steps: list[AnalyzeStep] = Field(default_factory=list)
    logs: list[str] = Field(default_factory=list)

class LogEntry(BaseModel):
    """
    Represents a single log entry for a tool, action, or reflection step.
    
    Attributes:
        step_type: The type of step ("tool", "action", or "reflection").
        tool: Name of the tool for tool or reflection steps (optional).
        action_type: Type of action for action steps (optional).
        plan_id: The ID of the plan this log belongs to.
        content: The log message, possibly joined from multiple lines.
    """
    step_type: AnalysisStepType
    tool: Optional[str] = None
    action_type: Optional[ActionType] = None
    plan_id: int
    content: str


class ReportSubsection(BaseModel):
    """Represents a subsection within a report section."""
    title: str = Field(description="Name of the subsection (e.g., 'Step 1: Data Cleaning')")
    content: str = Field(description="Content of the subsection")

class ReportSection(BaseModel):
    """Represents a section of the report."""
    title: str = Field(description="Name of the section (e.g., 'Plan 1 Summary')")
    content: str = Field(description="Content of the section")
    input_tokens: int = 0
    output_tokens: int = 0
    # subsections: Optional[ReportSubsection] = Field(default_factory=list, description="Nested subsections, if any")

class ReportSectionNoToken(BaseModel):
    """Represents a section of the report."""
    title: str = Field(description="Name of the section (e.g., 'Plan 1 Summary')")
    content: str = Field(description="Content of the section")


class ReportSectionInput(TypedDict):
    document_id: str
    model_id: str
    llm_key: str
    max_tokens: int
    original_variables: list[Variable] = Field(default_factory=list)
    current_variables: list[Variable] = Field(default_factory=list)
    execution_log: Optional[ExecutionLog] = Field(default_factory=dict)
    current_plan: Optional[Plan] = None
    queries: list['Query'] = Field(default_factory=list)
    docs: list['Doc'] = Field(default_factory=list)
    logs: list[LogEntry] = Field(default_factory=list)
    report_sections: Annotated[list[str], operator.add] = Field(
        default_factory=list,
        description="list of generated report sections, accumulated via addition"
    )
    filter_params: Optional[dict] = Field(default=None, description="Parameters to filter logs for this section")
    section_index: int = 1
    detailed_report: bool
    detailed_report_sections: Annotated[list[ReportSection], operator.add] = Field(
        default_factory=list,
        description="list of generated report sections, accumulated via addition"
    ) 
    

class State(TypedDict):
    # model_config = ConfigDict(arbitrary_types_allowed=True, underscore_attrs_are_private=True)
    model_id: str
    language: str
    llm_key: str
    max_tokens: int
    messages: Annotated[list[AnyMessage], add_messages] = []
    original_data: str  # Store as CSV string
    current_data: str  # Store as CSV string
    original_variables: Annotated[list[Variable], operator.add] = Field(default_factory=list)
    current_variables: list[Variable] = Field(default_factory=list)
    execution_log: Optional[ExecutionLog] = None
    current_plan: Optional[Plan] = None
    generated_files: dict[str, str] = Field(default_factory=dict)
    file_descriptions: dict[str, dict[str, str]] = Field(default_factory=dict)
    queries: list['Query'] = Field(default_factory=list)
    docs: list['Doc'] = Field(default_factory=list)
    plan_mode: ExecutionMode = ExecutionMode.AUTO
    action_mode: ExecutionMode = ExecutionMode.AUTO
    logs: list[LogEntry] = Field(default_factory=list)
    report_sections: Annotated[list[str], operator.add] = Field(
        default_factory=list,
        description="list of generated report sections, accumulated via addition"
    )
    input_tokens: int = 0
    output_tokens: int = 0
    final_report: str
    get_detailed_report: bool
    detailed_report_sections: Annotated[list[ReportSection], operator.add] = Field(
        default_factory=list,
        description="list of generated report sections, accumulated via addition"
    )
    detailed_report: list[str]
    available_tools: list[dict] = Field(default_factory=list)
    final_proposal: dict
    document_id: str
    # @classmethod
    # def from_dataframes(cls, original_data: pd.DataFrame, current_data: pd.DataFrame, **kwargs):
    #     """Create a State instance from DataFrames, converting them to CSV strings."""
    #     return cls(
    #         original_data=original_data.to_csv(index=False),
    #         current_data=current_data.to_csv(index=False),
    #         **kwargs
    #     )

    # def get_original_data(self) -> pd.DataFrame:
    #     """Convert CSV string back to DataFrame."""
    #     return pd.read_csv(io.StringIO(self.original_data))

    # def get_current_data(self) -> pd.DataFrame:
    #     """Convert CSV string back to DataFrame."""
    #     return pd.read_csv(io.StringIO(self.current_data))

class VariableCondition(BaseModel):
    condition_name: str = Field(description="Name/identifier of the condition being evaluated")
    explanation: str = Field(description="The reasoning behind the evaluation, serving as an explanation or thought process for how the conclusion was reached.")
    conclusion: bool = Field(description="Boolean result - True if condition is satisfied, False otherwise")