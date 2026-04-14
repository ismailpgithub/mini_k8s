CREATE TABLE "jobs" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"image" text NOT NULL,
	"cmd" text DEFAULT null,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp
);
