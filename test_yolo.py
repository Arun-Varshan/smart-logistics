# test_yolo.py
from ultralytics import YOLO
import os

# 1. Load your model file (yolov8n.pt is pre-trained, not custom)
model = YOLO("yolov8n.pt")   # or full path if needed: YOLO("path/to/yolov8n.pt")

# Print what classes this model knows (should be 80 COCO classes)
print("Model classes:", model.names)           # Shows dict {0: 'person', 1: 'bicycle', ...}
print("Number of classes:", len(model.names))

# 2. Pick one of your photos to test (change number if needed)
test_image = "uploads/parcel1.jpg"   # ← Change to parcel5.jpg or any

if not os.path.exists(test_image):
    print(f"Image not found: {test_image}")
else:
    print(f"\nTesting image: {test_image}")
    
    # Run prediction
    results = model.predict(
        source=test_image,      # your photo
        conf=0.25,              # confidence threshold (lower = more detections)
        iou=0.45,               # for non-max suppression
        show=True               # ← This opens a window with boxes & labels! (best for testing)
    )
    
    # Show results in terminal
    for result in results:
        print("\nDetected objects:")
        if len(result.boxes) == 0:
            print("→ Nothing detected")
        else:
            for box in result.boxes:
                cls_id = int(box.cls)
                class_name = result.names[cls_id]
                conf = float(box.conf)
                print(f"→ {class_name} | confidence: {conf:.2f}")

    print("\nDone! If window opened → look at the image with red boxes & labels.")