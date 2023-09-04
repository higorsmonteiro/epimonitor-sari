# import required modules
from tkinter import *
from tkinter import filedialog as fd
import os
from PIL import Image
from tkinter import messagebox
   
# create TK object 
root = Tk()
   
# naming the GUI interface to image_conversion_APP
root.title("Image_Conversion_App")
   
 
# function to convert jpg to gif
def img_to_gif():
    global im1
   
    # import the image from the folder
    import_filename = fd.askopenfilename()
    if import_filename.endswith(".jpg") or import_filename.endswith(".png"):
   
        im1 = Image.open(import_filename)
   
        # after converting the image save to desired
        # location with the Extersion .png
        export_filename = fd.asksaveasfilename(defaultextension=".gif")
        im1.save(export_filename)
   
        # displaying the Messaging box with the Success
        messagebox.showinfo("success ", "your Image converted to GIF Format")
    else:
   
        # if Image select is not with the Format of .jpg 
        # then display the Error
        Label_2 = Label(root, text="Error!", width=20,
                        fg="red", font=("bold", 15))
        Label_2.place(x=80, y=280)
        messagebox.showerror("Fail!!", "Something Went Wrong...")
   
 
# function to convert gif to jpg 
def gif_to_jpg():
    global im1
    import_filename = fd.askopenfilename()
   
    if import_filename.endswith(".gif"):
            im1=Image.open(import_filename).convert('RGB')
            export_filename=fd.asksaveasfilename(defaultextension=".jpg")
            im1.save(export_filename)
            messagebox.showinfo("Success","File converted to .jpg")
             
    else:
        messagebox.showerror("Fail!!","Error Interrupted!!!! Check Again")
 
 
# Driver Code
 
# add buttons
button1 = Button(root, text="Escolher Imagem", width=10,
                 height=2, bg="green", fg="white",
                 font=("helvetica", 12, "bold"),
                 command=img_to_gif)
   
button1.place(x=100, y=100)
   
#button2 = Button(root, text="GIF_to_JPG", width=20,
#                 height=2, bg="green", fg="white",
#                 font=("helvetica", 12, "bold"),
#                 command=gif_to_jpg)
   
#button2.place(x=120, y=220)
 
# adjust window size
root.geometry("300x300+120+120")
root.mainloop()