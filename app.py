from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.responses import HTMLResponse, RedirectResponse
from uvicorn import run as app_run

from typing import Optional

from heart_disease.constants import APP_HOST, APP_PORT
from heart_disease.pipline.prediction_pipeline import HeartDieseaseData, HeartDiseaseClassifier
from heart_disease.pipline.training_pipeline import TrainingPipeline




app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory='templates')

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



class DataForm:
    def __init__(self, request: Request):
        self.request: Request = request
        self.age: Optional[int] = None
        self.sex: Optional[str] = None
        self.cp: Optional[str] = None
        self.trestbps: Optional[int] = None
        self.restecg: Optional[str] = None
        self.thalch: Optional[int] = None
        self.exang: Optional[str] = None
        self.oldpeak: Optional[float] = None
        self.slope: Optional[str] = None

    async def get_heartdisease_data(self):
        form = await self.request.form()
        
        # Numeric conversion
        self.age = int(form.get("age"))
        self.trestbps = int(form.get("trestbps"))
        self.thalch = int(form.get("thalch"))
        self.oldpeak = float(form.get("oldpeak"))
        
        # Categorical: keep exact string as in training
        self.sex = form.get("sex")
        self.cp = form.get("cp")
        self.restecg = form.get("restecg")
        self.exang = form.get("exang")      # Must be "TRUE" / "FALSE"
        self.slope = form.get("slope")


        


@app.get("/", tags=["authentication"])
async def index(request: Request):

    return templates.TemplateResponse(
            "heartdisease.html",{"request": request, "context": "Rendering"})




@app.get("/train")
async def trainRouteClient():
    try:
        train_pipeline = TrainingPipeline()

        train_pipeline.run_pipeline()

        return Response("Training successful !!")

    except Exception as e:
        return Response(f"Error Occurred! {e}")
    



@app.post("/")
async def predictRouteClient(request: Request):
    try:
        form = DataForm(request)
        await form.get_heartdisease_data()
        
        # Create dataframe compatible with trained pipeline
        heartdisease_data = HeartDieseaseData(
            age=form.age,
            sex=form.sex,
            cp=form.cp,
            trestbps=form.trestbps,
            restecg=form.restecg,
            thalch=form.thalch,
            exang=form.exang,
            oldpeak=form.oldpeak,
            slope=form.slope
        )
        
        heartdisease_df = heartdisease_data.get_heartdisease_input_data_frame()
        print("Input DataFrame for prediction:\n", heartdisease_df.dtypes)
        print(heartdisease_df)

        # Predict
        model_predictor = HeartDiseaseClassifier()
        pred_array = model_predictor.predict(dataframe=heartdisease_df)
        value = int(pred_array[0])
        print("prectict value: ", value)           # Check if model is loaded


        # Map prediction to human readable
        status_map = {
            0: "No heart disease",
            1: "Mild heart disease",
            2: "Moderate heart disease",
            3: "Severe heart disease",
            4: "High-risk heart disease"
        }

        status = status_map.get(value, "Unknown")

        return templates.TemplateResponse(
            "heartdisease.html",
            {"request": request, "context": status},
        )
        
    except Exception as e:
        return {"status": False, "error": f"{e}"}

    



if __name__ == "__main__":
    app_run(app, host=APP_HOST, port=APP_PORT)